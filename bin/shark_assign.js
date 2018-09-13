#!/usr/bin/env node
/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2018, Joyent, Inc.
 */

var mod_crypto = require('crypto');
var mod_fs = require('fs');
var mod_path = require('path');
var mod_stream = require('stream');
var mod_util = require('util');

var mod_assertplus = require('assert-plus');
var mod_bunyan = require('bunyan');
var mod_getopt = require('posix-getopt');
var mod_jsprim = require('jsprim');
var mod_manta = require('manta');
var mod_moray = require('moray');
var mod_vasync = require('vasync');
var mod_verror = require('verror');

var EventEmitter = require('events').EventEmitter;
var LineStream = require('lstream');
var MemoryStream = require('memorystream');

var lib_rebalancer = require('../lib/rebalancer');

function SharkAssign(options) {
        mod_assertplus.object(options, 'options');
        mod_assertplus.object(options.log, 'options.log');
        mod_assertplus.string(options.host, 'options.host');
        mod_assertplus.string(options.owner, 'options.owner');
        mod_assertplus.arrayOfString(options.shards, 'options.shards');
        mod_assertplus.string(options.storageShard, 'options.storageShard');
        mod_assertplus.optionalString(options.newHost, 'options.newHost');
        mod_assertplus.optionalNumber(options.target, 'options.target');
        mod_assertplus.optionalNumber(options.split, 'options.split');
        mod_assertplus.optionalUuid(options.verify, 'options.verify');
        mod_assertplus.object(options.manta, 'options.manta');
        mod_assertplus.string(options.manta.user, 'options.manta.user');

        var self = this;

        self.sa_log = options.log;
        self.sa_host = options.host;
        self.sa_owner = options.owner;
        self.sa_metadata_shards = options.shards;
        self.sa_storage_shard = options.storageShard;
        self.sa_target = options.target || null;
        self.sa_split = options.split || 10000;
        self.sa_new_host = options.newHost || null;

        self.sa_verify = (options.verify) ? true : false;
        if (self.sa_verify) {
                self.sa_marker = options.verify || null;
                self.sa_started = true;
        } else {
                self.sa_marker = null;
                self.sa_started = false;
        }

        self.sa_manta_user = options.manta.user;

        self.sa_working_directory = mod_util.format(
            '/%s/stor/manta_shark_assign/do/%s/',
            self.sa_manta_user, self.sa_host);

        self.sa_progress_fmt = '%d-MOV-X-%d-X-%d-X-%s-X-%s';

        self.sa_progress = {
            'bytes': 0,
            'count': 0,
            'seq': 0
        };

        self.sa_move = {
            'bytes': 0,
            'entries': []
        };
        self.sa_sharks = {};
        self.sa_moray_clients = {
            'storage': null,
            'metadata': {}
        };
        self.sa_moray_connect_timeout = 1000;
        self.sa_moray_port = 2020;

        self.sa_input_stream = process.stdin;
        self.sa_work_stream = new mod_stream.Writable({ objectMode: true });
        self.sa_manta_client = mod_manta.createClient(options.manta);

        EventEmitter.call(self);

        self.connect();
}
mod_util.inherits(SharkAssign, EventEmitter);

SharkAssign.prototype.connect = function () {
        var self = this;

        mod_vasync.pipeline({ funcs: [
            function connectToStorageShard(_, next) {
                var client = mod_moray.createClient({
                    log: self.sa_log,
                    connectTimeout: self.sa_moray_connect_timeout,
                    host: self.sa_storage_shard,
                    port: self.sa_moray_port
                });
                client.on('error', next);
                client.on('connect', function () {
                        self.sa_moray_clients['storage'] = client;
                        self.sa_log.info({
                            shard: self.sa_storage_shard
                        }, 'connected to storage shard');
                        next();
                });
            },
            function connectToMetadataShards(_, next) {
                mod_vasync.forEachParallel({
                    inputs: self.sa_metadata_shards,
                    func: function (shard, cb) {
                        var client = mod_moray.createClient({
                            log: self.sa_log,
                            connectTimeout: self.sa_moray_connect_timeout,
                            host: shard,
                            port: self.sa_moray_port
                        });
                        client.on('error', cb);
                        client.on('connect', function () {
                                self.sa_moray_clients['metadata'][shard] =
                                    client;
                                cb();
                        });
                    }
                }, function (err) {
                        if (err) {
                                next(err);
                                return;
                        }
                        self.sa_log.info({
                            shards: self.sa_metadata_shards
                        }, 'connected to metadata shards');
                        next();
                });
            },
            function init(_, next) {
                self.init(next);
            }
        ] }, function (err) {
                if (err) {
                        self.emit('error', err);
                        return;
                }
                self.emit('connect');
        });
};

SharkAssign.prototype.init = function (callback) {
        var self = this;

        mod_vasync.pipeline({ funcs: [
            function getSharks(_, next) {
                self.getApplicableSharks(next);
            },
            function setupWorkingDirectory(_, next) {
                self.sa_manta_client.mkdirp(self.sa_working_directory, next);
            },
            function getProgress(_, next) {
                if (self.sa_verify) {
                        next();
                        return;
                }
                self.getProgress(next);
            }
        ] }, callback);
};

SharkAssign.prototype.start = function () {
        var self = this;

        self.sa_log.info({
            verify: self.sa_verify
        }, 'starting work');

        var line_stream = new LineStream();

        self.sa_work_stream._write = self.lookupObject();

        var pipeline = self.sa_input_stream.pipe(line_stream)
            .pipe(self.sa_work_stream);

        pipeline.on('finish', function () {
                self.sa_log.info('pipeline is finished');
                self.sync(function (err) {
                        if (err) {
                                self.emit('error', err);
                                return;
                        }
                        self.emit('done');
                });
        });

        self.sa_work_stream.on('enough', function () {
                self.emit('done');
        });

        self.sa_work_stream.on('error', function (err) {
                self.emit('error', err);
        });
};

SharkAssign.prototype.stop = function () {
        var self = this;

        self.close();

        self.sa_stopped = new Date();
};

SharkAssign.prototype.close = function () {
        var self = this;

        self.sa_manta_client.close();
        self.sa_moray_clients['storage'].close();
        mod_jsprim.forEachKey(self.sa_moray_clients['metadata'],
            function (shard, client) {
                client.close();
        });
};

SharkAssign.prototype.wantSync = function () {
        var self = this;

        return (self.sa_move.entries.length > 0 &&
            self.sa_move.entries.length % self.sa_split === 0 &&
            !self.sa_verify);
};

SharkAssign.prototype.getApplicableSharks = function (callback) {
        var self = this;

        var sharks = {};
        var sharksFound = 0;

        var req = self.sa_moray_clients['storage'].findObjects('manta_storage',
            '(manta_storage_id=*)', {});

        req.once('error', callback);

        req.on('record', function (obj) {
                var dc = obj.value.datacenter;
                var mantaStorageId = obj.value.manta_storage_id;
                var percentUsed = obj.value.percentUsed;

                //Filter out host if we're migrating away from it.
                if (mantaStorageId === self.sa_host) {
                        return;
                }
                /*
                 * XXX This is wasteful.  If we're looking for a particular host
                 * then we should just drop that into the query to moray.
                 */
                if (self.sa_new_host && mantaStorageId !== self.sa_new_host) {
                        return;
                }
                /*
                 * XXX Should this be a CLI option?
                 */
                if (percentUsed > 50) {
                        return;
                }

                if (!sharks[dc]) {
                        sharks[dc] = [];
                }

                sharksFound++;
                sharks[dc].push({
                    'manta_storage_id': mantaStorageId,
                    'datacenter': dc
                });
        });

        req.once('end', function () {
                self.sa_log.debug({
                    sharks: sharks
                }, 'full shark list');

                if (sharksFound === 0) {
                        callback(new mod_verror.VError('found no sharks'));
                        return;
                }

                self.sa_sharks = sharks;

                var per_az = {};

                Object.keys(self.sa_sharks).forEach(function (az) {
                        per_az[az] = self.sa_sharks[az].length;
                });

                self.sa_log.info({
                    sharksPerAz: per_az
                }, 'got sharks');

                callback();
        });
};

SharkAssign.prototype.getProgress = function (callback) {
        var self = this;

        self.getProgressNamesManta(function (err, names) {
                if (err) {
                        callback(err);
                        return;
                }

                if (names.length === 0) {
                        self.sa_started = new Date();
                        self.sa_log.info({
                            progress: self.progress()
                        }, 'no progress found; starting from scratch');
                        callback();
                        return;
                }

                var seqs = [];
                var files = {};
                names.forEach(function (f) {
                        if (mod_jsprim.endsWith(f, '.tmp')) {
                                return;
                        }
                        var s = f.split('-X-');
                        var seq = mod_jsprim.parseInteger(s[0].split('-')[0]);
                        seqs.push(seq);
                        files[seq] = {
                            count: mod_jsprim.parseInteger(s[1]),
                            bytes: mod_jsprim.parseInteger(s[2]),
                            marker: s[3]
                        };
                });

                seqs.sort(function (a, b) {
                    return (a - b);
                });

                if (seqs[0] !== 0) {
                    self.sa_log.warn({
                        files: files
                    }, 'sequence does not start at 0');
                    callback(new mod_verror.VError('found progress, but some ' +
                        'has been consumed.  perhaps you want the "-V" ' +
                        'option?'));
                    return;
                }

                seqs.forEach(function (seq) {
                        var f = files[seq];
                        self.sa_progress['count'] += f.count;
                        self.sa_progress['bytes'] += f.bytes;
                        self.sa_marker = f.marker;
                });

                self.sa_progress['seq'] = seqs[seqs.length - 1] + 1;

                self.sa_log.info({
                    progress: self.sa_progress
                }, 'got progress; continuing');

                callback();
        });
};

SharkAssign.prototype.lookupObject = function () {
        var self = this;

        return (function lookupObject(uuid, _, callback) {
                if (self.sa_target &&
                    self.sa_progress.bytes >= self.sa_target) {
                        self.sa_log.info({
                            progress: self.progress()
                        }, 'made enough progress; stopping');
                        self.sa_work_stream.emit('enough');
                        return;
                }
                if (uuid === self.sa_marker && !self.sa_started) {
                        self.sa_log.info({
                            progress: self.progress()
                        }, 'marker found');
                        self.sa_started = true;
                        callback();
                        return;
                }
                if (!self.sa_started) {
                        callback();
                        return;
                }

                mod_vasync.waterfall([
                    function doLookup(next) {
                        var lookup = {
                                objectid: uuid,
                                owner: self.sa_owner
                        };

                        var options = {
                            clients: self.sa_moray_clients['metadata'],
                            log: self.sa_log,
                            concurrency: 1
                        };

                        findObjectOnline(options, lookup, next);
                    }, function writeItOut(metadata, next) {
                        if (!metadata) {
                                next();
                                return;
                        }

                        if (metadata.value.sharks.length === 0) {
                                /*
                                 * XXX Job metadata?  Why does it not have
                                 * sharks?
                                 */
                                next();
                                return;
                        }

                        var sharks = metadata.value.sharks;
                        var foundShark = false;
                        sharks.forEach(function (shark) {
                                if (shark.manta_storage_id === self.sa_host) {
                                        foundShark = true;
                                }
                        });

                        if (!foundShark) {
                                next();
                                return;
                        }

                        if (self.sa_verify) {
                                self.sa_log.warn({
                                    sharks: sharks,
                                    metadata: metadata
                                }, 'found stuff to do in verification mode');
                        }
                        /*
                         * XXX Make it look like pg transform, but only some?
                         */
                        var fauxPg = {
                            //_id: metadata._id,
                            //_txn_snap: metadata._txn_snap,
                            //_mtime: metadata._mtime,
                            //_vnode: metadata._vnode,
                            //dirname: metadata.value.dirname,
                            //name: metadata.value.name,
                            //owner: metadata.value.owner,

                            _key: metadata.key,
                            _value: metadata.value,
                            _etag: metadata._etag,
                            objectid: metadata.value.objectId,
                            type: metadata.value.type
                        };

                        self.sa_move.bytes += metadata.value.contentLength;
                        self.sa_move.entries.push(fauxPg);

                        next();

                    }, function trySync(next) {
                        if (self.sa_verify && uuid === self.sa_marker) {
                                self.sa_log.info({
                                    marker: self.sa_marker
                                }, 'found marker in verification mode');
                                self.sa_work_stream.emit('enough');
                                return;
                        }

                        if (!self.wantSync()) {
                                next();
                                return;
                        }

                        self.sync(next);
                    }
                ], callback);
        });
};

SharkAssign.prototype.sync = function (callback) {
        var self = this;

        if (self.sa_move.entries.length === 0) {
                self.sa_log.info({
                    count: self.sa_move.entries.length,
                    bytes: self.sa_move.bytes
                }, 'nothing to sync');
                callback();
                return;
        }

        mod_vasync.pipeline({ funcs: [
            function assignNewShark(_, cb) {
                self.assignNewShark(cb);
            },
            function saveProgress(_, cb) {
                self.saveProgressManta(cb);
            }
        ] }, callback);
};

SharkAssign.prototype.progress = function () {
        var self = this;

        var rv = {
            'GiB': Math.round((self.sa_progress.bytes / 1024 / 1024 / 1024)),
            'objects': self.sa_progress.count
        };

        return (rv);
};

SharkAssign.prototype.saveProgressManta = function (callback) {
        var self = this;

        var objectId = self.sa_move.entries[
            self.sa_move.entries.length - 1]._value.objectId;

        var filename = mod_util.format(self.sa_progress_fmt,
            self.sa_progress.seq,
            self.sa_move.entries.length,
            self.sa_move.bytes,
            objectId,
            self.sa_owner);

        self.sa_log.info({
            location: self.sa_working_directory + filename
        }, 'saving progress to manta');

        self.sa_manta_client.mkdirp(self.sa_working_directory, function (err) {
                if (err) {
                        callback(err);
                        return;
                }

                var data = '';
                var md5 = mod_crypto.createHash('md5').update(data)
                    .digest('base64');

                self.sa_move.entries.forEach(function (e) {
                        data += JSON.stringify(e, null, 0) + '\n';
                });
                var o = {
                    copies: 2,
                    md5: md5,
                    size: Buffer.byteLength(data)
                };
                var mstream = new MemoryStream();
                var wstream = self.sa_manta_client.createWriteStream(
                    self.sa_working_directory + filename, o);

                mstream.pipe(wstream);

                wstream.once('error', callback);
                wstream.once('close', function () {

                        self.updateProgress();

                        self.sa_log.info({
                            location: self.sa_working_directory + filename,
                            progress: self.progress()
                        }, 'progress saved');

                        callback();
                });
                mstream.end(data);
        });
};

SharkAssign.prototype.updateProgress = function () {
        var self = this;

        self.sa_progress.seq++;
        self.sa_progress.bytes += self.sa_move.bytes;
        self.sa_progress.count += self.sa_move.entries.length;
        self.sa_marker = self.sa_move.entries[
            self.sa_move.entries.length - 1]._value.objectId;

        self.sa_move.entries = [];
        self.sa_move.bytes = 0;
};

SharkAssign.prototype.assignNewShark = function (callback) {
        var self = this;

        var errors = [];
        var reassigned = 0;

        self.sa_move.entries.forEach(function (o) {
                var assignment, err;

                try {
                    assignment = lib_rebalancer.checkForMantaStorageId(
                        o,
                        self.sa_sharks,
                        self.sa_host);
                } catch (e) {
                    err = new mod_verror.VError(e, 'failed to assign shark ' +
                        'for objectid "%s"', o._value.objectId);
                    self.sa_log.debug({
                        object: o
                    }, err.message);
                }

                if (err) {
                        errors.push(err);
                        return;
                }

                mod_assertplus.object(assignment.oldShark,
                    'assignment.oldShark');
                mod_assertplus.object(assignment.newShark,
                    'assignment.newShark');

                o.oldShark = assignment.oldShark;
                o.newShark = assignment.newShark;
                o.key = o._value.key;
                o.morayEtag = o._etag;
                o.md5 = o._value.contentMD5;
                o.objectId = o._value.objectId;
                o.owner = o._value.owner;
                o.etag = o._value.etag;

                reassigned++;
        });

        /*
         * XXX These shouldn't really be assertions, but properly handled and
         * explained via callbacks.
         */
        mod_assertplus.equal(errors.length, 0, 'there were errors!');
        mod_assertplus.equal(reassigned, self.sa_move.entries.length,
            'not enough reassigned');

        callback();
};

SharkAssign.prototype.getProgressNamesManta = function (callback) {
        var self = this;

        var names = [];
        /*
         * XXX pagination is missing
         */
        self.sa_manta_client.ls(self.sa_working_directory, { type: 'object' },
            function (err, res) {
                if (err) {
                        callback(err);
                        return;
                }
                res.on('object', function (o) {
                        if (o.name.indexOf(self.sa_owner) !== -1) {
                                names.push(o.name);
                        }
                });
                res.once('error', callback);
                res.once('end', function () {
                        callback(null, names);
                });
        });
};

/*
 * This should probably be in lib/common.js, but it's not an interface that
 * should really be exposed too much for now, and keeping it here actually helps
 * with the deployment process of what this tool was originally written for.
 */
function findObjectOnline(opts, lookup, callback) {
        mod_assertplus.object(opts, 'opts');
        mod_assertplus.object(opts.clients, 'opts.clients');
        mod_assertplus.object(opts.log, 'opts.log');
        mod_assertplus.optionalNumber(opts.concurrency, 'opts.concurrency');
        mod_assertplus.object(lookup, 'lookup');
        mod_assertplus.uuid(lookup.owner, 'lookup.owner');
        mod_assertplus.uuid(lookup.objectid, 'lookup.objectid');
        mod_assertplus.func(callback, 'callback');

        var log = opts.log;

        /*
         * This is quite arbitrary, but is ultimately intended to just
         * be a low number.
         */
        var concurrency = opts.concurrency || 3;

	var rv = null;
        var results = null;
        var errors = [];

	log.debug({
	    lookup: lookup
	}, 'starting lookup');

        /*
         * XXX it would be good if we dropped out of this function as soon as we
         * get a response, as opposed to waiting for every shard to respond with
         * nothing.  Perhaps this doesn't make a difference?
         */
        var queue = mod_vasync.queue(function (shard, next) {
                var c = opts.clients[shard];
                /*
                 * "objectId" is case sensitive here, but I was seeing results
                 * with a lowercase "objectid".  The results I was getting were
                 * not reliable, and heading to a moray zone for the applicable
                 * shard and running this query also returned no results.
                 *
                 * Why was I sometimes getting a result?  FWIW, I never seemed
                 * to get a result for the same objectid twice.
                 */
                var query = '(&(owner=' + lookup.owner + ')(objectId=' +
                    lookup.objectid + '))';

                log.debug({
                    lookup: lookup,
                    query: query,
                    shard: shard
                }, 'looking up object');

                var req = c.findObjects('manta', query, {});

                req.once('error', function (err) {
                        errors.push(new mod_verror.VError(err,
                            'failed to query "%s"', shard));
                        next();
                        return;
                });

                req.on('record', function (obj) {
			if (!results) {
				results = {};
			}
			if (!results[shard]) {
				results[shard] = [];
			}
                        results[shard].push(obj);
                });

                req.once('end', function () {
                        next();
                        return;
                });
        }, concurrency);

        queue.push(Object.keys(opts.clients));
        queue.close();

        queue.on('end', function () {
                if (!results) {
                        log.debug({ lookup: lookup }, 'found no object');
			callback(mod_verror.errorFromList(errors), rv);
			return;
		}

		log.debug({ results: results }, 'results');

		if (Object.keys(results).length === 1 &&
		    results[Object.keys(results)[0]].length === 1) {
			/*
			 * No dups, no snaplinks.  Return the only object.
			 */
			rv = results[Object.keys(results)[0]][0];
			log.debug({ rv: rv }, 'found object');
			callback(mod_verror.errorFromList(errors), rv);
			return;
		}

		var etagConflict = false;
		var etag = null;
		var foundSnaplinks = null;
		mod_jsprim.forEachKey(results, function (shard, objects) {
			if (objects.length > 1) {
				/*
				 * XXX Snaplinks aren't supported yet.
				 */
				log.debug({
				    shard: shard,
				    objects: objects
				}, 'snaplinks are not supported yet');
				foundSnaplinks = true;
			}

			var object = objects[0];
			if (!etag) {
				etag = object._etag;
			}
			if (etag !== object._etag) {
				errors.push(new mod_verror.VError('found ' +
				    'objects in different shards with unique ' +
				    'etags'));
				etagConflict = true;
			}
		});

		if (foundSnaplinks) {
			callback(new mod_verror.VError('snaplinks ' +
			    'are not supported yet'));
			return;
		}

		if (!etagConflict) {
			log.debug({
			    results: results
			}, 'found multiple objects in different ' +
			    'shards, but no etag conflict');
			rv = results[Object.keys(results)[0]][0];
		}

		log.debug({ rv: rv }, 'found object');
                callback(mod_verror.errorFromList(errors), rv);
        });
}



function parseOptions(opts) {
        var option;

        opts.shards = opts.shards || [];
        var parser = new mod_getopt.BasicParser('o:d:t:x:h:n:V:v',
            process.argv);
        while ((option = parser.getopt()) !== undefined) {
                if (option.error) {
                        usage();
                }

                switch (option.option) {
                case 'o':
                        opts.owner = option.optarg;
                        break;
                case 't':
                        opts.target = option.optarg;
                        break;
                case 'x':
                        opts.split = option.optarg;
                        break;
                case 'h':
                        opts.host = option.optarg;
                        break;
                case 'V':
                        /*
                         * If this isn't a UUID then we're expecting to verify
                         * the full contents of the input stream.
                         */
                        if (!option.optarg) {
                                opts.verify = true;
                        } else {
                                opts.verify = option.optarg;
                        }
                        break;
                case 'n':
                        opts.newHost = option.optarg;
                        break;
                case 'v':
                        opts.verbose = true;
                        break;
                default:
                        usage('Unknown option: ' + option.option);
                        break;
                }
        }

        if (!opts.storageShard) {
                usage('Storage shard is required.');
        }

        if (!opts.host) {
                usage('Host is required.');
        }

        if (opts.target && opts.verify) {
                usage('-t and -V cannot be used together.');
        }

        if (opts.split) {
                opts.split = mod_jsprim.parseInteger(opts.split);
        }

        if (opts.target) {
                /*
                 * Convert GiB input value to bytes.
                 */
                opts.target = opts.target * 1024 * 1024 * 1024;
        }

        /*
         * XXX We could also do with a marker in other situations.  For example,
         * if a pass has been fully completed and objects moved but we want to
         * move more based on the same input list, we could make use of a marker
         * to determine how much of the input to skip, as opposed to reaching
         * out to the metadata tier for all of them.
         */

        /*
         * XXX Split won't do anything if verify is passed.  Prevent both
         * options?
         */

        return (opts);
}

function usage(msg) {
        if (msg) {
                console.error(msg);
        }
        var str  = 'usage: ' + mod_path.basename(process.argv[1]);
        str += ' [-o] owner_uuid';
        str += ' [-h] storage_id';
        str += ' [-n] storage_id';
        str += ' [-t] target_in_gib';
        str += ' [-x] split';
        str += ' [-s] storage_shard';
        str += ' [-V] uuid';
        str += ' [-v]';
        console.error(str);
        process.exit(1);
}

function main() {
        var config_path = (process.env.MOLA_REBALANCE_CONFIG ||
            '/opt/smartdc/mola/etc/config.json');
        var config = JSON.parse(
            mod_fs.readFileSync(config_path));

        var options = parseOptions(config);

        var log = mod_bunyan.createLogger({
            'name': 'sharkassign',
            'level': (options.verbose) ? 'debug' : 'info'
        });

        log.info({
            options: options
        }, 'got options');

        options.log = log;

        var assigner = new SharkAssign(options);

        assigner.on('connect', function () {
                assigner.start();
        });

        assigner.on('error', function (err) {
                log.fatal(err, 'failed to reassign input');
                assigner.stop();
        });

        assigner.on('done', function () {
                assigner.stop();
                log.info('done');
        });
}

main();
