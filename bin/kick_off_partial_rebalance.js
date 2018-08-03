#!/usr/bin/env node
/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2018, Joyent, Inc.
 */

/*
 * Current plan
 *
 * - Pull down the mako dump for the affected storage node and store it in
 *   memory (it's ~30MiB)
 * - Work through this dump 10 lines at a time and reach out to each metadata
 *   shard, ensuring only 10 shards a queuried at a time.  We don't want this
 *   thing to impact the system.  These values should be tuneable
 * - If we find a metadata entry, store this metadata in a list of objects that
 *   need to be moved
 * - If we don't find a metadata entry, store this objectid and size in a list
 *   of objects that have potential for GC
 * - At 1000 objects in either list, write the list to a Manta object somewhere
 *     - We could also store the total size of all objects and total number of
 *       objects, maybe encoded in the object's name.  The intention here is to
 *       help with resumability, where the program will just need to pull the
 *       object count and storage size from the path, and not by pulling the
 *       object itself
 * - Once we've reached our target in GiB, stop and write whatever lists we have
 *   on this pass of 1000 objects to Manta
 *
 * Program inputs:
 *
 * - Storage ID
 * - Amount of data to be freed in GiB
 * - Ignore sharks
 * - Dry run
 * - Test
 * - How many passes of the loop it should do? E.g. 3 == 3000 objects queried
 * - Concurrency
 *
 * Once we have these lists in Manta we can consume them and transform them
 * into input that the rebalance or GC pipeline could consume.  This might be
 * worth doing on first pass of generating the lists, but perhaps not as there's
 * valuable information (running size, number of objects scanned) that is
 * important to the idempotency of this program.
 *
 * Which subsequent pipeline we go for (rebalance or GC) depends on the total
 * amount of storage size that we're estimating (knowing?) will be freed.  Given
 * the outputs defined above, I think this should be quite straight forward to
 * tell.  It would be good to do a run of this on a sample of objects (say, the
 * first 5k) to get a good first estimate of progress.
 *
 * Speaking of progress, this needs to be considered.  The program itself should
 * report how far through the 1000 entries it is to stdout while running.  It
 * could also have a mode to simply report what objects it finds in Manta and
 * tally up the numbers.
 *
 * Implementation:
 *
 * /poseidon/stor/manta_rebalance/pre/0-MOV-X-NOBJECTS-X-NBYTES-X-LASTUUID
 * /poseidon/stor/manta_rebalance/pre/0-DEL-X-NOBJECTS-X-NBYTES-X-LASTUUID
 *
 * e.g.
 * /poseidon/stor/manta_rebalance/pre/99999-MOV-X-28748329-X-381240172340713247021347234-X-60361888-9413-11e8-9a79-172969bd78d3
 *
 * /poseidon/stor/manta_rebalance/do/N.stor.etc/XXXXX
 */

var bunyan = require('bunyan');
var common = require('../lib/common');
var fs = require('fs');
var getopt = require('posix-getopt');
var lib = require('../lib');
var manta = require('manta');
var moray = require('moray');
var path = require('path');
var vasync = require('vasync');
var jsprim = require('jsprim');
var crypto = require('crypto');
var util = require('util');
var EventEmitter = require('events').EventEmitter;
var carrier = require('carrier');
var stream = require('stream');
var verror = require('verror');
var LineStream = require('lstream');
var vstream = require('vstream');

var format = util.format;



///--- Global Objects

var NAME = 'mola-partial-rebalance';
var LOG = bunyan.createLogger({
        level: (process.env.LOG_LEVEL || 'info'),
        name: NAME,
        stream: process.stdout
});
var MOLA_REBALANCE_CONFIG = (process.env.MOLA_REBALANCE_CONFIG ||
                   '/opt/smartdc/mola/etc/config.json');
var MOLA_REBALANCE_CONFIG_OBJ = JSON.parse(
        fs.readFileSync(MOLA_REBALANCE_CONFIG));
var MANTA_CLIENT = manta.createClientFromFileSync(MOLA_REBALANCE_CONFIG, LOG);
var MANTA_USER = MANTA_CLIENT.user;
var MORAY_BUCKET = 'manta_storage';
var MORAY_CONNECT_TIMEOUT = 1000;
var MORAY_PORT = 2020;



///--- Global Constants
var MP = '/' + MANTA_USER + '/stor';
var MANTA_DUMP_NAME_PREFIX = 'manta-';
var SHARKS_ASSET_FILE = '/var/tmp/mola-rebalance-sharks.json';
var MAKO_DUMP_PATH = MP + '/mako';

var TARGET = 1 * 1024 * 1024 * 1024; // 1GiB?
var MAX_MAKO_ROWS = 100000000;


///--- Helpers

/* BEGIN JSSTYLED */
function getEnvCommon(opts) {
        return (' \
set -o pipefail && \
cd /assets/ && gtar -xzf ' + opts.marlinPathToAsset + ' && cd mola && \
');
}
/* END JSSTYLED */


/* BEGIN JSSTYLED */
/**
 * The map phase takes the pgdump, filters out all the directories
 * and formats them like:
 * [objectid] { [json pg row data] }
 */
function getMapCmd(opts) {
        return (getEnvCommon(opts) + ' \
gzcat -f | ./build/node/bin/node ./bin/pg_transform.js | \
   ./build/node/bin/node ./bin/jext.js -f objectid -x | \
   msplit -d " " -f 1 -n ' + opts.numberReducers + ' \
');
}
/* END JSSTYLED */


/* BEGIN JSSTYLED */
function getRebalanceCmd(opts) {
        var sharksAsset = '/assets' + opts.sharksAssetObject;
        var tmpDir = '/var/tmp/sharkDist';
        var hostOption = '';
        if (opts.host) {
                hostOption = '-h ' + opts.host + ' ';
        }
        return (getEnvCommon(opts) + ' \
rm -rf ' + tmpDir + ' && \
mkdir ' + tmpDir + ' && \
sort | ./build/node/bin/node ./bin/jext.js -r | \
    ./build/node/bin/node ./bin/rebalance.js \
       -s ' + sharksAsset + ' -d ' + tmpDir + ' ' + hostOption + '&& \
for i in $(ls ' + tmpDir + '); do \
   mmkdir ' + opts.jobRoot + '/do/$i; \
   mput -f ' + tmpDir + '/$i \
     ' + opts.jobRoot + '/do/$i/$MANTA_JOB_ID-X-$(uuid); \
done \
');
}
/* END JSSTYLED */


function parseOptions() {
        var option;
        //First take what's in the config file, override what's on the
        // command line, and use the defaults if all else fails.
        var opts = MOLA_REBALANCE_CONFIG_OBJ;
        opts.shards = opts.shards || [];
        opts.ignoreSharks = [];
        var parser = new getopt.BasicParser('h:o:i:m:s',
                                            process.argv);
        while ((option = parser.getopt()) !== undefined) {
                if (option.error) {
                        usage();
                }

                switch (option.option) {
                case 'h':
                        opts.host = option.optarg;
                        break;
		case 'o':
			opts.owner = option.optarg;
			break;
                case 'i':
                        opts.ignoreSharks.push(option.optarg);
                        break;
                case 'm':
                        opts.shards.push(option.optarg);
                        break;
                case 's':
                        opts.storageShard = option.optarg;
                        break;
                default:
                        usage('Unknown option: ' + option.option);
                        break;
                }
        }

        if (!opts.storageShard) {
                usage('Storage shard is required.');
        }

        //Set up some defaults...
        opts.jobName = opts.jobName || 'manta_rebalance';
        opts.jobRoot = opts.jobRoot || MP + '/manta_rebalance';
        opts.directories = [
                opts.jobRoot + '/do'
        ];
        opts.assetDir = opts.jobRoot + '/assets';
        opts.assetObject = opts.assetDir + '/mola.tar.gz';
        opts.sharksAssetObject = opts.assetDir + '/sharks.json';
        opts.assetFile = opts.assetFile ||
                '/opt/smartdc/common/bundle/mola.tar.gz';

        opts.rebalanceMemory = opts.rebalanceMemory || 4096;
        opts.marlinPathToAsset = opts.assetObject.substring(1);
        opts.marlinAssetObject = opts.assetObject;

        return (opts);
}


function usage(msg) {
        if (msg) {
                console.error(msg);
        }
        var str  = 'usage: ' + path.basename(process.argv[1]);
        str += ' [-a asset_file]';
        str += ' [-h manta_storage_id]';
        str += ' [-m moray_shard]';
        str += ' [-n]';
        str += ' [-r marlin_memory]';
        str += ' [-s storage_shard]';
        str += ' [-t]';
        console.error(str);
        process.exit(1);
}


//Pulls the current set of active sharks and uploads to manta.  I probably
// should have the job manager do this, but whatever.
function makeSharksAsset(opts, cb) {
        getCurrentSharks(opts, function (err, sharks) {
                if (err) {
                        cb(err);
                        return;
                }
                if (Object.keys(sharks).length < 1) {
                        var message = 'no active sharks found.';
                        cb(new Error(message));
                        return;
                }
                //TODO: Could just use an in-memory object...
                fs.writeFileSync(SHARKS_ASSET_FILE, JSON.stringify(sharks));
                var stats = fs.statSync(SHARKS_ASSET_FILE);
                var o = {
                        copies: 2,
                        size: stats.size
                };

                var s = fs.createReadStream(SHARKS_ASSET_FILE);
                var p = opts.sharksAssetObject;
                s.pause();
                s.on('open', function () {
                        MANTA_CLIENT.put(p, s, o, function (e) {
                                fs.unlinkSync(SHARKS_ASSET_FILE);
                                cb(e);
                        });
                });
        });
}



function getCurrentSharks(opts, cb) {
        var client = moray.createClient({
                log: LOG,
                connectTimeout: MORAY_CONNECT_TIMEOUT,
                host: opts.storageShard,
                port: MORAY_PORT
        });

        client.on('connect', function () {
                var sharks = {};
                var req = client.findObjects(MORAY_BUCKET,
                                             '(manta_storage_id=*)', {});

                req.once('error', function (err) {
                        cb(err);
                        return;
                });

                req.on('record', function (obj) {
                        var dc = obj.value.datacenter;
                        var mantaStorageId = obj.value.manta_storage_id;
                        //Filter out host if we're migrating away from it.
                        if (mantaStorageId === opts.host) {
                                return;
                        }
                        //Filter out other ignore hosts
                        if (opts.ignoreSharks.indexOf(mantaStorageId) !== -1) {
                                return;
                        }
                        if (!sharks[dc]) {
                                sharks[dc] = [];
                        }
                        sharks[dc].push({
                                'manta_storage_id': mantaStorageId,
                                'datacenter': dc
                        });
                });

                req.once('end', function () {
                        client.close();
                        cb(null, sharks);
                });
        });
}



function getMakoDump(opts, callback) {
	/*
	 * XXX This should really emit a stream of objects/lines
	 */
	var dump;
	MANTA_CLIENT.get(MAKO_DUMP_PATH + '/' + opts.host,
	    function (err, o) {
		if (err) {
			callback(err);
			return;
		}
		o.on('data', function (d) {
			dump += d;
		});
		o.once('error', function (err) {
			callback(err);
		});
		o.once('end', function () {
			callback(null, dump);
		});
	});
}




function getProgress(opts, cb) {

}



///--- Main

var _opts = parseOptions();

var objectsToMove = {
    totalSize: 0,
    entries: {}
};

var movStream = fs.createWriteStream('/var/tmp/testingMov');
//var delStream = fs.createWriteStream('/var/tmp/testingDel');
_opts.clients = {};
_opts.log = LOG;
//_opts.started = true;
_opts.marker = 'd294c8ce-3bd8-e93a-a0cc-b9a44e410319';

vasync.waterfall([
    function connectToMorays(cb) {
	vasync.forEachParallel({
	    func: function (m, next) {
		var client = moray.createClient({
		    log: _opts.log,
		    connectTimeout: MORAY_CONNECT_TIMEOUT,
		    host: m,
		    port: MORAY_PORT
		});
		client.once('error', function (err) {
			LOG.error(err);
			next(err);
			return;
		});
		client.once('connect', function () {
			_opts.clients[m] = client;
			next();
		});
	    },
	    inputs: _opts.shards
	}, function (err, res) {
		LOG.info('connected to shards');
		cb(err);
	});
    },
    function getProgress(cb) {
	/*
	 * Reach out to Manta and pull the name of the last object we wrote,
	 * which will tell us the last UUID, running total size, and how many
	 * objects.
	 */
	cb(null, null);
    },
    function startWork(progress, cb) {
        var lstream = new LineStream();
        var workStream = new stream.Writable({ objectMode: true });
        workStream._write = lookupObject;

	process.stdin.pipe(lstream).pipe(workStream);

        workStream.on('finish', function () {
                cb();
        });
    }
], function (err) {
	LOG.info({
	    move: objectsToMove.totalSize,
	}, 'sizes');
	LOG.info({
	    moveInGiB: objectsToMove.totalSize / 1024 / 1024 / 1024
	}, 'sizes in GiB');
	finish(err);
});

function lookupObject(uuid, _, callback) {
        if (uuid === _opts.marker && !_opts.started) {
                LOG.info({
		    marker: _opts.marker
		}, 'marker found');
                _opts.started = true;
                callback();
                return;
        }
        if (!_opts.started) {
                callback();
                return;
        }

        var lookup = {
                objectid: uuid,
                owner: _opts.owner
        };
        common.findObjectOnline(_opts, lookup, function (err, metadata) {
                if (err) {
                        callback(err);
                        return;
                }
                if (metadata) {
                        /*
                         * make it look like pg transform
                         */
                        var fauxPg = {
                            _id: metadata._id,
                            _txn_snap: metadata._txn_snap,
                            _key: metadata.key,
                            _value: metadata.value,
                            _etag: metadata.etag,
                            _mtime: metadata._mtime,
                            _vnode: metadata._vnode,
                            dirname: metadata.value.dirname,
                            name: metadata.value.name,
                            owner: metadata.value.owner,
                            objectid: metadata.value.objectid,
                            type: metadata.value.type
                        }

                        objectsToMove.totalSize += metadata.value.contentLength;
                        movStream.write(JSON.stringify(fauxPg, null, 0) + '\n');
		}

                callback();
        });
}

function finish(err) {
        if (err) {
                LOG.fatal(err);
                process.exit(1);
        }
        MANTA_CLIENT.close();
	Object.keys(_opts.clients).forEach(function (shard) {
		_opts.clients[shard].close();
	});
        LOG.info('Done for now.');
        process.exit(0);
}
