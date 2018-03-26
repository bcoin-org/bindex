/*!
 * chainclient.js - chain client for bcoin
 * Copyright (c) 2014-2017, Christopher Jeffrey (MIT License).
 * https://github.com/bcoin-org/bcoin
 */

'use strict';

const assert = require('assert');
const AsyncEmitter = require('bevent');
const CoinView = require('bcoin').coins.CoinView;

/**
 * Node Client
 * @alias module:lib.ChainClient
 */

class ChainClient extends AsyncEmitter {
  /**
   * Create a chain client.
   * @constructor
   */

  constructor(chain) {
    super();

    this.chain = chain;
    this.network = chain.network;
    this.opened = false;

    this.init();
  }

  /**
   * Initialize the client.
   */

  init() {
    this.chain.on('connect', (entry, block, view) => {
      if (!this.opened)
        return;

      this.emit('block connect', entry, block, view);
    });

    this.chain.on('disconnect', (entry, block, view) => {
      if (!this.opened)
	return;

      this.emit('block disconnect', entry, block, view);
    });

    this.chain.on('reset', (tip) => {
      if (!this.opened)
	return;

      this.emit('chain reset', tip);

    });
  }

  /**
   * Open the client.
   * @returns {Promise}
   */

  async open(options) {
    assert(!this.opened, 'ChainClient is already open.');
    this.opened = true;
    setImmediate(() => this.emit('connect'));
  }

  /**
   * Close the client.
   * @returns {Promise}
   */

  async close() {
    assert(this.opened, 'ChainClient is not open.');
    this.opened = false;
    setImmediate(() => this.emit('disconnect'));
  }

  /**
   * Add a listener.
   * @param {String} type
   * @param {Function} handler
   */

  bind(type, handler) {
    return this.on(type, handler);
  }

  /**
   * Add a listener.
   * @param {String} type
   * @param {Function} handler
   */

  hook(type, handler) {
    return this.on(type, handler);
  }

  /**
   * Get chain tip.
   * @returns {Promise}
   */

  async getTip() {
    return this.chain.tip;
  }


  /**
   * Get chain entry.
   * @param {Hash} hash
   * @returns {Promise}
   */

  async getEntry(hash) {
    const entry = await this.chain.getEntry(hash);

    if (!entry)
      return null;

    if (!await this.chain.isMainChain(entry))
      return null;

    return entry;
  }

  /**
   * Get block
   * @param {Hash} hash
   * @returns {Promise}
   */

  async getBlock(hash) {
    const block = await this.chain.getBlock(hash);

    if (!block)
      return null;

    return block;
  }

  /**
   * Spend and update inputs.
   * @private
   * @param {Block} block
   * @param {ChainEntry} prev
   * @returns {Promise} - Returns {@link CoinView}.
   */

  async updateInputs(block, prev) {
    const view = new CoinView();
    const height = prev.height + 1;
    const cb = block.txs[0];

    view.addTX(cb, height);

    for (let i = 1; i < block.txs.length; i++) {
      const tx = block.txs[i];

      assert(await view.spendInputs(this.db, tx),
        'BUG: Spent inputs in historical data!');

      view.addTX(tx, height);
    }

    return view;
  }

  /**
   * Get previous entry.
   * @param {ChainEntry} entry
   * @returns {Promise} - Returns ChainEntry.
   */

  getPrevious(entry) {
    return this.chain.getEntryByHash(entry.prevBlock);
  }

  /**
   * Get a historical block coin viewpoint.
   * @param {Block} hash
   * @returns {Promise} - Returns {@link CoinView}.
   */

  async getBlockView(block) {
    const prev = await this.getPrevious(block);
    const view = await this.updateInputs(block, prev);
    return view;
  }
}

/*
 * Expose
 */

module.exports = ChainClient;
