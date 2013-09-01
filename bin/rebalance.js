#!/usr/bin/env node
// -*- mode: js -*-
// Copyright 2012 Joyent, Inc.  All rights reserved.

var getopt = require('posix-getopt');
var fs = require('fs');
var lib = require('../lib');
var path = require('path');
var util = require('util');



///--- Helpers

function parseOptions() {
        var option;
        var opts = {};
        var parser = new getopt.BasicParser('d:s:',
                                            process.argv);
        while ((option = parser.getopt()) !== undefined && !option.error) {
                switch (option.option) {
                case 'd':
                        opts.tmpDirectory = option.optarg;
                        break;
                case 's':
                        opts.sharksFile = option.optarg;
                        break;
                default:
                        usage('Unknown option: ' + option.option);
                        break;
                }
        }

        if (!opts.tmpDirectory) {
                usage('-d [tmp directory] is a required argument');
        }

        if (!opts.sharksFile) {
                usage('-s [sharks file] is a required argument');
        }

        return (opts);
}


function usage(msg) {
        if (msg) {
                console.error(msg);
        }
        var str  = 'usage: ' + path.basename(process.argv[1]);
        str += '-d [tmp directory]';
        str += '-s [sharks file]';
        str += '';
        console.error(str);
        process.exit(1);
}



///--- Main

var _opts = parseOptions();
_opts.reader = process.stdin;

var _sharks = JSON.parse(fs.readFileSync(_opts.sharksFile));
var _rebalancer = lib.createRebalancer({
        reader: process.stdin,
        sharks: _sharks,
        dir: _opts.tmpDirectory
});

_rebalancer.on('error', function (err) {
        console.error(err);
        process.exit(1);
});

process.stdin.resume();