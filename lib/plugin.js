/*!
 * plugin.js - index plugin for bcoin
 * Copyright (c) 2014-2017, Christopher Jeffrey (MIT License).
 * https://github.com/bcoin-org/bcoin
 */

'use strict';

const Index = require('./index');

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
    const options = {
      network: node.network,
      logger: node.logger,
      chain: node.chain,
      prefix: node.config.filter('index').prefix,
      memory: node.config.filter('index').bool('memory', node.memory),
      maxFiles: node.config.filter('index').uint('max-files'),
      cacheSize: node.config.filter('index').mb('cache-size'),
      indexTX: node.config.filter('index').bool('tx'),
      indexAddress: node.config.filter('index').bool('address'),
      indexFilters: node.config.filter('index').bool('filters')
    };

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
