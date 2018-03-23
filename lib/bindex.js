/*!
 * bindex.js - index for bcoin
 * Copyright (c) 2014-2017, Christopher Jeffrey (MIT License).
 * https://github.com/bcoin-org/bcoin
 */

'use strict';

const Plugin = require('./plugin');
const Index = require('./index');
const ChainClient = require('./chainclient');

exports = Plugin;

exports.Plugin = Plugin;
exports.Index = Index;
exports.ChainClient = ChainClient;

module.exports = exports;
