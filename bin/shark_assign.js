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

        self.pr_log = options.log;
        self.pr_host = options.host;
        self.pr_owner = options.owner;
        self.pr_metadata_shards = options.shards;
        self.pr_storage_shard = options.storageShard;
        self.pr_split = options.split || 10000;
        self.pr_new_host = options.newHost || null;

        self.pr_verify = (options.verify) ? true : false;
        if (self.pr_verify) {
                self.pr_marker = options.verify;
                self.pr_started = true;
                self.pr_target = options.null;
        } else {
                self.pr_marker = null;
                self.pr_started = false;
                self.pr_target = options.target;
        }

        self.pr_manta_user = options.manta.user;

        self.pr_working_directory = mod_util.format(
            '/%s/stor/manta_shark_assign/do/%s/',
            self.pr_manta_user, self.pr_host);

        self.pr_progress_fmt = '%d-MOV-X-%d-X-%d-X-%s';

        self.pr_progress = {
            'bytes': 0,
            'count': 0,
            'seq': 0
        };

        self.pr_move = {
            'bytes': 0,
            'entries': []
        };
        self.pr_sharks = {};
        self.pr_moray_clients = {
            'storage': null,
            'metadata': {}
        };
        self.pr_moray_connect_timeout = 1000;
        self.pr_moray_port = 2020;

        self.pr_input_stream = process.stdin;
        self.pr_work_stream = new mod_stream.Writable({ objectMode: true });
        self.pr_manta_client = mod_manta.createClient(options.manta);

        EventEmitter.call(self);

        self.connect();
}
mod_util.inherits(SharkAssign, EventEmitter);

SharkAssign.prototype.connect = function () {
        var self = this;

        mod_vasync.pipeline({ funcs: [
            function connectToStorageShard(_, next) {
                var client = mod_moray.createClient({
                    log: self.pr_log,
                    connectTimeout: self.pr_moray_connect_timeout,
                    host: self.pr_storage_shard,
                    port: self.pr_moray_port
                });
                client.on('error', next);
                client.on('connect', function () {
                        self.pr_moray_clients['storage'] = client;
                        self.pr_log.info({
                            shard: self.pr_storage_shard
                        }, 'connected to storage shard');
                        next();
                });
            },
            function connectToMetadataShards(_, next) {
                mod_vasync.forEachParallel({
                    inputs: self.pr_metadata_shards,
                    func: function (shard, cb) {
                        var client = mod_moray.createClient({
                            log: self.pr_log,
                            connectTimeout: self.pr_moray_connect_timeout,
                            host: shard,
                            port: self.pr_moray_port
                        });
                        client.on('error', cb);
                        client.on('connect', function () {
                                self.pr_moray_clients['metadata'][shard] =
                                    client;
                                cb();
                        });
                    }
                }, function (err) {
                        if (err) {
                                next(err);
                                return;
                        }
                        self.pr_log.info({
                            shards: self.pr_metadata_shards
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
                self.pr_manta_client.mkdirp(self.pr_working_directory, next);
            },
            function getProgress(_, next) {
                if (self.pr_verify) {
                        next();
                        return;
                }
                self.getProgress(next);
            }
        ] }, callback);
};

SharkAssign.prototype.start = function () {
        var self = this;

        self.pr_log.info({
            verify: self.pr_verify
        }, 'starting work');

        var line_stream = new LineStream();

        self.pr_work_stream._write = self.lookupObject();

        self.pr_input_stream.pipe(line_stream).pipe(self.pr_work_stream);

        self.pr_work_stream.on('end', function () {
                self.pr_log.info({
                }, 'reached the end of input stream');

                self.emit('done');
        });

        self.pr_work_stream.on('enough', function () {
                self.emit('done');
        });

        self.pr_work_stream.on('error', function (err) {
                self.emit('error', err);
        });
};

SharkAssign.prototype.stop = function () {
        var self = this;

        self.close();

        self.pr_stopped = new Date();
};

SharkAssign.prototype.close = function () {
        var self = this;

        self.pr_manta_client.close();
        self.pr_moray_clients['storage'].close();
        mod_jsprim.forEachKey(self.pr_moray_clients['metadata'],
            function (shard, client) {
                client.close();
        });
};

SharkAssign.prototype.wantSync = function () {
        var self = this;

        return (self.pr_move.entries.length > 0 &&
            self.pr_move.entries.length % self.pr_split === 0 &&
            !self.pr_verify);
};

SharkAssign.prototype.getApplicableSharks = function (callback) {
        var self = this;

        var sharks = {};
        var sharksFound = 0;

        var req = self.pr_moray_clients['storage'].findObjects('manta_storage',
            '(manta_storage_id=*)', {});

        req.once('error', callback);

        req.on('record', function (obj) {
                var dc = obj.value.datacenter;
                var mantaStorageId = obj.value.manta_storage_id;
                var percentUsed = obj.value.percentUsed;

                //Filter out host if we're migrating away from it.
                if (mantaStorageId === self.pr_host) {
                        return;
                }
                /*
                 * XXX This is wasteful.  If we're looking for a particular host
                 * then we should just drop that into the query to moray.
                 */
                if (self.pr_new_host && mantaStorageId !== self.pr_new_host) {
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
                self.pr_log.debug({
                    sharks: sharks
                }, 'full shark list');

                if (sharksFound === 0) {
                        callback(new mod_verror.VError('found no sharks'));
                        return;
                }

                self.pr_sharks = sharks;

                var per_az = {};

                Object.keys(self.pr_sharks).forEach(function (az) {
                        per_az[az] = self.pr_sharks[az].length;
                });

                self.pr_log.info({
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
                        self.pr_started = new Date();
                        self.pr_log.info({
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
                    self.pr_log.warn({
                        files: files
                    }, 'sequence does not start at 0');
                    callback(new mod_verror.VError('found progress, but some ' +
                        'has been consumed.  perhaps you want the "-V" ' +
                        'option?'));
                    return;
                }

                seqs.forEach(function (seq) {
                        var f = files[seq];
                        self.pr_progress['count'] += f.count;
                        self.pr_progress['bytes'] += f.bytes;
                        self.pr_marker = f.marker;
                });

                self.pr_progress['seq'] = seqs[seqs.length - 1] + 1;

                callback();
        });
};

SharkAssign.prototype.lookupObject = function () {
        var self = this;

        return (function lookupObject(uuid, _, callback) {
                if (self.pr_progress.bytes >= self.pr_target) {
                        self.pr_log.info({
                            progress: self.progress()
                        }, 'made enough progress; stopping');
                        self.pr_work_stream.emit('enough');
                        return;
                }
                if (uuid === self.pr_marker && !self.pr_started) {
                        self.pr_log.info({
                            progress: self.progress()
                        }, 'marker found');
                        self.pr_progress.started = true;
                        callback();
                        return;
                }
                if (!self.pr_started) {
                        callback();
                        return;
                }

                if (self.pr_verify && uuid === self.pr_marker) {
                        self.pr_log.info({
                            marker: self.pr_marker
                        }, 'found marker in verification mode');
                        self.pr_work_stream.emit('enough');
                        return;
                }

                mod_vasync.waterfall([
                    function doLookup(next) {
                        var lookup = {
                                objectid: uuid,
                                owner: self.pr_owner
                        };

                        var options = {
                            clients: self.pr_moray_clients['metadata'],
                            log: self.pr_log,
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
                                if (shark.manta_storage_id === self.pr_host) {
                                        foundShark = true;
                                }
                        });

                        if (!foundShark) {
                                next();
                                return;
                        }

                        if (self.pr_verify) {
                                self.pr_log.warn({
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
                            objectid: metadata.value.objectid,
                            type: metadata.value.type
                        };

                        self.pr_move.bytes += metadata.value.contentLength;
                        self.pr_move.entries.push(fauxPg);

                        next();

                    }, function trySync(next) {
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
            'GiB': Math.round((self.pr_progress.bytes / 1024 / 1024 / 1024)),
            'objects': self.pr_progress.count
        };

        return (rv);
};

SharkAssign.prototype.saveProgressManta = function (callback) {
        var self = this;

        var objectId = self.pr_move.entries[
            self.pr_move.entries.length - 1]._value.objectId;

        var filename = mod_util.format(self.pr_progress_fmt,
            self.pr_progress.seq,
            self.pr_move.entries.length,
            self.pr_move.bytes,
            objectId);

        self.pr_log.info({
            location: self.pr_working_directory + filename
        }, 'saving progress to manta');

        self.pr_manta_client.mkdirp(self.pr_working_directory, function (err) {
                if (err) {
                        callback(err);
                        return;
                }

                var data = '';
                var md5 = mod_crypto.createHash('md5').update(data)
                    .digest('base64');

                self.pr_move.entries.forEach(function (e) {
                        data += JSON.stringify(e, null, 0) + '\n';
                });
                var o = {
                    copies: 2,
                    md5: md5,
                    size: Buffer.byteLength(data)
                };
                var mstream = new MemoryStream();
                var wstream = self.pr_manta_client.createWriteStream(
                    self.pr_working_directory + filename, o);

                mstream.pipe(wstream);

                wstream.once('error', callback);
                wstream.once('close', function () {

                        self.updateProgress();

                        self.pr_log.info({
                            location: self.pr_working_directory + filename,
                            progress: self.progress()
                        }, 'progress saved');

                        callback();
                });
                mstream.end(data);
        });
};

SharkAssign.prototype.updateProgress = function () {
        var self = this;

        self.pr_progress.seq++;
        self.pr_progress.bytes += self.pr_move.bytes;
        self.pr_progress.count += self.pr_move.entries.length;
        self.pr_marker = self.pr_move.entries[
            self.pr_move.entries.length - 1]._value.objectId;

        self.pr_move.entries = [];
        self.pr_move.bytes = 0;
};

SharkAssign.prototype.assignNewShark = function (callback) {
        var self = this;

        var errors = [];
        var reassigned = 0;

        self.pr_move.entries.forEach(function (o) {
                var assignment = lib_rebalancer.checkForMantaStorageId(
                    o,
                    self.pr_sharks,
                    self.pr_host);

                if (!assignment) {
                        errors.push({
                            object: o
                        });
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
        mod_assertplus.equal(reassigned, self.pr_move.entries.length,
            'not enough reassigned');

        callback();
};

SharkAssign.prototype.getProgressNamesManta = function (callback) {
        var self = this;

        var names = [];
        /*
         * XXX pagination is missing
         */
        self.pr_manta_client.ls(self.pr_working_directory, { type: 'object' },
            function (err, res) {
                if (err) {
                        callback(err);
                        return;
                }
                res.on('object', function (o) {
                        names.push(o.name);
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
        var errors = [];
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
                        errors.push(err);
                        next();
                        return;
                });

                req.on('record', function (obj) {
                        rv = obj;
                });

                req.once('end', function () {
                        next();
                        return;
                });
        }, concurrency);

        queue.push(Object.keys(opts.clients));
        queue.close();

        queue.on('end', function () {
                if (!rv) {
                        log.debug({ lookup: lookup }, 'found no object');
                } else {
                        log.debug({ object: rv }, 'found object');
                }
                callback(mod_verror.errorFromList(errors), rv);
        });
}



function parseOptions(opts) {
        var option;

        opts.shards = opts.shards || [];
        var parser = new mod_getopt.BasicParser('o:d:t:x:h:V:n:v',
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
                         * XXX Could very well be a bool if explicitly
                         * reassigning, or if we'de like to verify the full
                         * input stream.
                         */
                        opts.verify = option.optarg;
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

        /*
         * XXX Target is only required if not verifying and not explicitly
         * reassigning.
         * XXX In fact, target is entirely optional?
         */
        if (!opts.target && !opts.verify) {
                usage('Target (in GiB) is required.');
        }

        if (!opts.host) {
                usage('Host is required.');
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

        return (opts);
}

function usage(msg) {
        if (msg) {
                console.error(msg);
        }
        var str  = 'usage: ' + mod_path.basename(process.argv[1]);
        str += ' [-u] owner_uuid';
        str += ' [-d] working_directory';
        str += ' [-t] target_in_gib';
        str += ' [-x] split';
        str += ' [-i] ignore_sharks';
        str += ' [-s] storage_shard';
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
