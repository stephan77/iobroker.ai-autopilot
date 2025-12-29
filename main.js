
'use strict';
const utils = require('@iobroker/adapter-core');
const OpenAI = require('openai');

class AiAutopilot extends utils.Adapter {
  constructor(options={}) {
    super({...options, name:'ai-autopilot'});
    this.on('ready', ()=>this.onReady());
  }

  async onReady() {
    await this.setObjectNotExistsAsync('info.connection', {
      type:'state',
      common:{type:'boolean', role:'indicator.connected', read:true, write:false},
      native:{}
    });
    await this.setStateAsync('info.connection', true, true);

    if (this.config.debug) {
      this.log.info('[DEBUG] Debug logging enabled');
    }

    this.log.info('AI Autopilot v0.5.8 ready');
  }
}

if (module.parent) {
  module.exports = options => new AiAutopilot(options);
} else {
  new AiAutopilot();
}
