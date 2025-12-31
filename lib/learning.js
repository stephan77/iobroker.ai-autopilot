'use strict';

module.exports = () => ({
  async processFeedback(feedback) {
    if (!this.pendingActions) {
      return;
    }

    const normalized = feedback.toUpperCase();
    if (normalized === 'JA') {
      await this.finalizeApproval('approved');
    } else if (normalized === 'NEIN') {
      await this.finalizeApproval('rejected');
    } else if (normalized.startsWith('ÄNDERN')) {
      this.log.info(`Änderungswunsch: ${feedback}`);
    }
  },

  async loadFeedbackHistory() {
    try {
      const existing = await this.getStateAsync('memory.feedback');
      if (!existing || !existing.val) {
        this.feedbackHistory = [];
        return;
      }
      const parsed = JSON.parse(String(existing.val));
      if (Array.isArray(parsed)) {
        this.feedbackHistory = parsed.slice(-50);
      } else if (parsed && typeof parsed === 'object') {
        this.feedbackHistory = [parsed].slice(-50);
      } else {
        this.feedbackHistory = [];
      }
    } catch (error) {
      this.feedbackHistory = [];
      this.handleError('Feedback Historie konnte nicht geladen werden', error, true);
    }
  },

  async loadLearningHistory() {
    try {
      const existing = await this.getStateAsync('memory.learning');
      if (!existing || !existing.val) {
        this.learningHistory = [];
        return;
      }
      const parsed = JSON.parse(String(existing.val));
      if (Array.isArray(parsed)) {
        this.learningHistory = parsed.slice(-200);
      } else if (parsed && typeof parsed === 'object') {
        this.learningHistory = [parsed].slice(-200);
      } else {
        this.learningHistory = [];
      }
    } catch (error) {
      this.learningHistory = [];
      this.handleError('Learning Historie konnte nicht geladen werden', error, true);
    }
  },

  async loadLearningHistoryEntries() {
    try {
      const existing = await this.getStateAsync('memory.history');
      if (!existing || !existing.val) {
        this.learningHistoryEntries = [];
        return;
      }
      const parsed = JSON.parse(String(existing.val));
      if (Array.isArray(parsed)) {
        this.learningHistoryEntries = parsed.slice(-500);
      } else if (parsed && typeof parsed === 'object') {
        this.learningHistoryEntries = [parsed].slice(-500);
      } else {
        this.learningHistoryEntries = [];
      }
    } catch (error) {
      this.learningHistoryEntries = [];
      this.handleError('Learning History konnte nicht geladen werden', error, true);
    }
  },

  aggregateLearningStats(entries) {
    const stats = {
      totals: { approved: 0, rejected: 0, executed: 0 },
      byKey: {}
    };
    for (const entry of entries || []) {
      if (!entry) {
        continue;
      }
      const decision = String(entry.decision || '').toLowerCase();
      if (!['approved', 'rejected', 'executed'].includes(decision)) {
        continue;
      }
      stats.totals[decision] += 1;
      const key = entry.learningKey || 'unknown';
      if (!stats.byKey[key]) {
        stats.byKey[key] = { approved: 0, rejected: 0, executed: 0, total: 0 };
      }
      stats.byKey[key][decision] += 1;
      stats.byKey[key].total += 1;
    }
    return stats;
  },

  buildFeedbackContext(context, energySummary) {
    const timeLabel = new Date(context.timestamp).toLocaleTimeString('de-DE', {
      hour: '2-digit',
      minute: '2-digit'
    });
    const outsideTemp = this.getOutsideTemperature(context?.live?.temperature || []);
    return {
      houseConsumption: energySummary.houseConsumption ?? null,
      gridPower: energySummary.gridPower ?? null,
      pvPower: energySummary.pvPower ?? null,
      batterySoc: energySummary.batterySoc ?? null,
      outsideTemp,
      timeOfDay: timeLabel
    };
  },

  buildFeedbackEntry(action, approvalResult, executionResult) {
    const executed = this.wasActionExecuted(executionResult);
    return {
      actionId: String(action.id ?? ''),
      category: this.normalizeFeedbackCategory(action.category),
      decision: approvalResult === 'approved' ? 'approved' : 'rejected',
      executed,
      timestamp: new Date().toISOString(),
      context: this.lastContextSummary || {
        houseConsumption: null,
        pvPower: null,
        batterySoc: null,
        outsideTemp: null,
        timeOfDay: new Date().toLocaleTimeString('de-DE', { hour: '2-digit', minute: '2-digit' })
      }
    };
  },

  buildLearningEntry(action, userDecision) {
    const context = action?.context || {};
    const timestamp = context.timestamp || new Date().toISOString();
    const timeOfDay = new Date(timestamp).toLocaleTimeString('de-DE', { hour: '2-digit', minute: '2-digit' });
    return {
      learningKey: String(action.learningKey || 'unknown'),
      actionType: String(action.type || 'unknown'),
      userDecision,
      decision: userDecision,
      context: {
        timeOfDay,
        batterySoc: context.batterySoc ?? null,
        outsideTemp: context.outsideTemp ?? null,
        pvPower: context.pvPower ?? null
      },
      contextSnapshot: { ...context },
      timestamp: new Date().toISOString()
    };
  },

  buildLearningHistoryEntry(action, decision) {
    const context = action?.context || {};
    return {
      learningKey: String(action.learningKey || 'unknown'),
      actionId: String(action.id || ''),
      decision,
      timestamp: new Date().toISOString(),
      context: {
        batterySoc: context.batterySoc ?? null,
        houseConsumption: context.houseConsumption ?? null,
        outsideTemp: context.outsideTemp ?? null
      }
    };
  },

  async storeFeedbackEntries(entries) {
    if (!Array.isArray(entries) || entries.length === 0) {
      return;
    }
    try {
      this.feedbackHistory = [...this.feedbackHistory, ...entries].slice(-50);
      await this.setStateAsync('memory.feedback', JSON.stringify(this.feedbackHistory, null, 2), true);
      if (this.config.debug) {
        this.log.info(`[DEBUG] Feedback stored (${entries.length} entries)`);
      }
    } catch (error) {
      this.log.warn(`Feedback konnte nicht gespeichert werden: ${error.message}`);
    }
  },

  async storeLearningEntries(entries) {
    if (!Array.isArray(entries) || entries.length === 0) {
      return;
    }
    try {
      this.learningHistory = [...this.learningHistory, ...entries].slice(-200);
      await this.setStateAsync('memory.learning', JSON.stringify(this.learningHistory, null, 2), true);
      if (this.config.debug) {
        this.log.info(`[DEBUG] Learning entry stored (${entries.length} entries)`);
      }
    } catch (error) {
      this.log.warn(`Learning konnte nicht gespeichert werden: ${error.message}`);
    }
  },

  async storeLearningHistoryEntries(entries) {
    if (!Array.isArray(entries) || entries.length === 0) {
      return;
    }
    try {
      this.learningHistoryEntries = [...this.learningHistoryEntries, ...entries].slice(-500);
      await this.setStateAsync('memory.history', JSON.stringify(this.learningHistoryEntries, null, 2), true);
      if (this.config.debug) {
        this.log.info(`[DEBUG] Learning entries stored (${entries.length} entries)`);
        this.log.info(`[DEBUG] Learning entries: ${JSON.stringify(entries, null, 2)}`);
      }
    } catch (error) {
      this.log.warn(`Learning History konnte nicht gespeichert werden: ${error.message}`);
    }
  },

  wasActionExecuted(executionResult) {
    const status = executionResult?.status;
    return status === 'success' || status === 'error';
  },

  normalizeFeedbackCategory(category) {
    const normalized = String(category || '').toLowerCase();
    const allowed = new Set(['energy', 'water', 'heating', 'pv', 'safety']);
    if (allowed.has(normalized)) {
      return normalized;
    }
    if (this.config.debug) {
      this.log.info(`[DEBUG] Unknown feedback category "${category}", defaulting to energy`);
    }
    return 'energy';
  },

  async storeActionLearningDecision(actionId, decision) {
    const actions = await this.loadActionsFromState();
    const action = actions.find((entry) => entry && String(entry.id) === String(actionId));
    if (!action) {
      return;
    }
    await this.storeLearningEntries([this.buildLearningEntry(action, decision)]);
  }
});
