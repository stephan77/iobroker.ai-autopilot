'use strict';

module.exports = () => ({
  async onStateChange(id, state) {
    if (!state) return;

    // IGNORIERE ACK EVENTS
    if (state.ack) return;

    if (id === this.namespace + '.control.run' && state.val === true) {
      if (this.config.debug) {
        this.log.info('[DEBUG] Trigger received: control.run');
      }

      // SOFORT zurücksetzen (Impuls!)
      await this.setStateAsync('control.run', false, true);
      if (this.config.debug) {
        this.log.info('[DEBUG] control.run reset');
      }

      // Mehrfachlauf verhindern
      if (this.running) {
        this.log.warn('Analysis already running, trigger ignored');
        return;
      }

      this.running = true;

      try {
        await this.runAnalysis(); // <-- HIER MUSS GPT AUFGERUFEN WERDEN
      } catch (e) {
        this.log.error('Analysis failed: ' + e.message);
      } finally {
        this.running = false;
      }
    }

    if (id === `${this.namespace}.memory.feedback`) {
      await this.processFeedback(String(state.val || '').trim());
    }
  },

  async runAnalysisWithLock(trigger) {
    if (this.running) {
      this.log.warn('Analysis already running, trigger ignored');
      return;
    }

    this.running = true;
    try {
      await this.runAnalysis(trigger);
    } finally {
      this.running = false;
    }
  }
});
