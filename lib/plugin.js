/*!
 * plugin.js - indexing plugin for bcoin
 * Copyright (c) 2014-2017, Christopher Jeffrey (MIT License).
 * https://github.com/bcoin-org/bcoin
 */

'use strict';

const Index = require('./index');
const ChainClient = require('./chainclient');

/**
 * @exports plugin
 */

const plugin = exports;

/**
 * Plugin
 * @extends Index
 */

class Plugin extends Index {
  /**
   * Create a plugin.
   * @constructor
   * @param {Node} node
   */

  constructor(node) {
    const config = node.config.filter('index');
    const options = {
      client: new ChainClient(node.chain),
      network: node.network.toString(),
      logger: node.logger,
      prefix: config.prefix,
      indexTX: config.bool('tx'),
      indexAddress: config.bool('address'),
      indexFilters: config.bool('filters'),
      memory: config.bool('memory', node.memory),
      maxFiles: config.uint('max-files'),
      cacheSize: config.mb('cache-size')
    }
    super(options);
  }
}

/**
 * Plugin name.
 * @const {String}
 */

plugin.id = 'bindex';

/**
 * Plugin initialization.
 * @param {Node} node
 * @returns {Plugin}
 */

plugin.init = function init(node) {
  return new Plugin(node);
};
