'use strict';

module.exports = () => ({
  dedupeActions(actions) {
    const uniqueMap = new Map();
    for (const action of actions || []) {
      if (!action) {
        continue;
      }
      const key =
        action.id ||
        `${action.category || 'unknown'}:${action.type || 'action'}:${action.learningKey || ''}:${action.deviationRef || ''}`;
      if (uniqueMap.has(key)) {
        this.logDebug('Duplicate action replaced', action);
      }
      uniqueMap.set(key, action);
    }
    return Array.from(uniqueMap.values());
  },

  async requestApproval(actions, reportText) {
    const sentActions = this.markActionsSent(actions);
    this.pendingActions = sentActions;
    await this.persistActions(sentActions, 'Pending actions saved');
    const approvalText = this.buildApprovalMessage(actions, reportText);
    await this.sendTelegramMessage(approvalText, {
      includeKeyboard: true,
      parseMode: 'Markdown',
      actions: actions
    });
  },

  async executeActions(actions) {
    const approvedActions = actions.filter((action) => action.status === 'approved');
    this.log.info(`Aktionen freigegeben: ${approvedActions.length}`);
    for (const action of approvedActions) {
      await this.executeAction(action);
    }

    await this.persistActions(actions, 'Actions executed');
    await this.storeFeedbackEntries(
      approvedActions.map((action) =>
        this.buildFeedbackEntry(action, 'approved', action.executionResult)
      )
    );
    await this.storeLearningEntries(
      approvedActions.map((action) => this.buildLearningEntry(action, 'approved'))
    );
    await this.storeLearningHistoryEntries(
      approvedActions.map((action) => this.buildLearningHistoryEntry(action, 'executed'))
    );
  },

  updateActionStatuses(actions, status) {
    for (const action of actions) {
      this.applyActionStatusTransition(action, status);
    }
  },

  markActionsSent(actions) {
    for (const action of actions) {
      const normalized = this.normalizeActionForPersistence(action);
      Object.assign(action, normalized);
    }
    return actions;
  },

  buildActionTimestamps(existing = {}) {
    const now = new Date().toISOString();
    return {
      createdAt: existing.createdAt || now,
      decidedAt: existing.decidedAt || null,
      executedAt: existing.executedAt || null
    };
  },

  normalizeActionForPersistence(action) {
    if (!action || typeof action !== 'object') {
      return action;
    }
    const normalizedStatus = this.normalizeActionStatus(action.status);
    const timestamps = this.buildActionTimestamps(action.timestamps || {});
    const decision = action.decision ?? null;
    return {
      ...action,
      status: normalizedStatus,
      decision,
      timestamps,
      learningKey: action.learningKey || 'unknown'
    };
  },

  normalizeActionStatus(status) {
    const normalized = String(status || 'proposed').toLowerCase();
    if (['proposed', 'approved', 'rejected', 'executed', 'failed'].includes(normalized)) {
      return normalized;
    }
    if (normalized === 'sent') {
      return 'proposed';
    }
    return 'proposed';
  },

  applyActionDecision(action, decision) {
    if (!action) {
      return false;
    }
    const now = new Date().toISOString();
    action.decision = decision;
    action.timestamps = this.buildActionTimestamps(action.timestamps || {});
    action.timestamps.decidedAt = action.timestamps.decidedAt || now;
    this.logDebug('Action decision updated', {
      actionId: action.id,
      decision
    });
    return true;
  },

  applyActionStatusTransition(action, nextStatus) {
    if (!action) {
      return false;
    }
    const currentStatus = this.normalizeActionStatus(action.status);
    const targetStatus = this.normalizeActionStatus(nextStatus);
    if (!this.isActionTransitionAllowed(currentStatus, targetStatus)) {
      this.log.warn(
        `Ungültige Status-Transition für Aktion ${action.id}: ${currentStatus} -> ${targetStatus}`
      );
      return false;
    }

    const now = new Date().toISOString();
    action.status = targetStatus;
    action.timestamps = this.buildActionTimestamps(action.timestamps || {});

    if (targetStatus === 'approved') {
      action.decision = 'approved';
      action.timestamps.decidedAt = action.timestamps.decidedAt || now;
    }

    if (targetStatus === 'rejected') {
      action.decision = 'rejected';
      action.timestamps.decidedAt = action.timestamps.decidedAt || now;
    }

    if (targetStatus === 'executed' || targetStatus === 'failed') {
      action.timestamps.executedAt = action.timestamps.executedAt || now;
    }

    this.logDebug('Action status changed', {
      actionId: action.id,
      from: currentStatus,
      to: targetStatus
    });
    return true;
  },

  isActionTransitionAllowed(currentStatus, nextStatus) {
    const transitions = {
      proposed: ['approved', 'rejected'],
      approved: ['executed', 'failed'],
      rejected: [],
      executed: [],
      failed: []
    };
    if (currentStatus === nextStatus) {
      return true;
    }
    return (transitions[currentStatus] || []).includes(nextStatus);
  },

  async persistActions(actions, debugLabel) {
    try {
      const normalized = (actions || []).map((action) => this.normalizeActionForPersistence(action));
      await this.setStateAsync('report.actions', JSON.stringify(normalized, null, 2), true);
      await this.updateActionHistory(normalized);
      this.logDebug(debugLabel || 'Actions persisted', { count: normalized.length });
    } catch (error) {
      this.handleError('Aktionen konnten nicht gespeichert werden', error, true);
    }
  },

  async updateActionHistory(actions) {
    try {
      const existing = await this.loadActionHistory();
      const historyMap = new Map();
      for (const entry of existing) {
        const key = entry?.id || entry?.actionId || null;
        if (key) {
          historyMap.set(String(key), entry);
        }
      }

      for (const action of actions || []) {
        if (!action) {
          continue;
        }
        const key = action.id || action.actionId;
        if (!key) {
          continue;
        }
        historyMap.set(String(key), { ...action, updatedAt: new Date().toISOString() });
      }

      const merged = Array.from(historyMap.values()).slice(-500);
      await this.setStateAsync('report.actionHistory', JSON.stringify(merged, null, 2), true);
      this.logDebug('Action history updated', { count: merged.length });
    } catch (error) {
      this.handleError('Aktionen-Historie konnte nicht gespeichert werden', error, true);
    }
  },

  async loadActionHistory() {
    const state = await this.getStateAsync('report.actionHistory');
    if (!state || !state.val) {
      return [];
    }
    try {
      const actions = JSON.parse(state.val);
      return Array.isArray(actions) ? actions : [];
    } catch (error) {
      this.handleError('Daily report action history parsing failed', error, true);
      return [];
    }
  },

  async persistLearningForDecision(action, status) {
    if (!action) {
      return;
    }
    const normalizedStatus = this.normalizeActionStatus(status);
    if (normalizedStatus === 'rejected') {
      await this.storeLearningHistoryEntries([this.buildLearningHistoryEntry(action, 'rejected')]);
      await this.storeLearningEntries([this.buildLearningEntry(action, 'rejected')]);
    }
    if (normalizedStatus === 'executed') {
      await this.storeLearningHistoryEntries([this.buildLearningHistoryEntry(action, 'executed')]);
      await this.storeLearningEntries([this.buildLearningEntry(action, 'approved')]);
    }
  },

  updatePendingAction(action) {
    if (!this.pendingActions) {
      return;
    }
    const pending = this.pendingActions.find((entry) => entry && String(entry.id) === String(action.id));
    if (pending) {
      Object.assign(pending, action);
    }
  },

  async loadActionsFromState() {
    const state = await this.getStateAsync('report.actions');
    if (!state || !state.val) {
      return [];
    }
    try {
      const parsed = JSON.parse(String(state.val));
      if (!Array.isArray(parsed)) {
        return [];
      }
      return parsed.map((action) => this.normalizeActionForPersistence(action));
    } catch (error) {
      this.handleError('Gespeicherte Aktionen konnten nicht geparst werden', error, true);
      return [];
    }
  },

  async updateActionStatusInState(actionId, status) {
    const actions = await this.loadActionsFromState();
    if (actions.length === 0) {
      this.log.warn('Keine gespeicherten Aktionen für Status-Update gefunden.');
      return;
    }
    const action = actions.find((entry) => entry && String(entry.id) === String(actionId));
    if (!action) {
      this.log.warn(`Aktion nicht gefunden für Status-Update: ${actionId}`);
      return;
    }

    const transitionApplied = this.applyActionStatusTransition(action, status);
    if (!transitionApplied) {
      return;
    }
    await this.persistActions(actions, 'Action status updated');
    if (this.pendingActions) {
      const pending = this.pendingActions.find((entry) => entry && String(entry.id) === String(actionId));
      if (pending) {
        this.applyActionStatusTransition(pending, status);
      }
    }

    await this.persistLearningForDecision(action, status);
  },

  getActionHandlers() {
    return {
      energy: async (action) => this.handleEnergyAction(action),
      heating: async (action) => this.handleHeatingAction(action),
      water: async (action) => this.handleWaterAction(action),
      pv: async (action) => this.handlePvAction(action),
      safety: async (action) => this.handleSafetyAction(action)
    };
  },

  async handleEnergyAction(action) {
    this.log.info(`Energie-Aktion: ${this.describeAction(action)}`);
    return { status: 'success', message: 'Energie-Aktion protokolliert' };
  },

  async handleHeatingAction(action) {
    this.log.info(`Heizungs-Aktion: ${this.describeAction(action)}`);
    return { status: 'success', message: 'Heizungs-Aktion protokolliert' };
  },

  async handleWaterAction(action) {
    this.log.info(`Wasser-Aktion: ${this.describeAction(action)}`);
    return { status: 'success', message: 'Wasser-Aktion protokolliert' };
  },

  async handlePvAction(action) {
    this.log.info(`PV-Aktion: ${this.describeAction(action)}`);
    return { status: 'success', message: 'PV-Aktion protokolliert' };
  },

  async handleSafetyAction(action) {
    this.log.info(`Sicherheits-Aktion: ${this.describeAction(action)}`);
    return { status: 'success', message: 'Sicherheits-Aktion protokolliert' };
  },

  async executeAction(action) {
    const handlers = this.getActionHandlers();
    const handler = handlers[action.category];
    this.logDebug('Action execution intent', {
      actionId: action.id,
      description: this.describeAction(action),
      dryRun: this.config.dryRun
    });

    try {
      if (this.config.dryRun) {
        action.executionResult = { status: 'skipped', reason: 'dryRun' };
      } else if (!handler) {
        action.executionResult = { status: 'skipped', reason: 'noHandler' };
      } else {
        const result = await handler(action);
        action.executionResult = result || { status: 'success' };
      }
      this.applyActionStatusTransition(action, 'executed');
    } catch (error) {
      action.executionResult = { status: 'error', message: error.message };
      this.applyActionStatusTransition(action, 'failed');
      this.handleError(`Aktion fehlgeschlagen: ${this.describeAction(action)}`, error, true);
    }
  },

  normalizeActionPriority(priority) {
    switch (String(priority || '').toLowerCase()) {
      case 'high':
        return 'high';
      case 'medium':
        return 'medium';
      default:
        return 'low';
    }
  },

  formatActionTitle(type) {
    if (!type) {
      return 'Aktion';
    }
    return String(type).replace(/_/g, ' ').replace(/\b\w/g, (char) => char.toUpperCase());
  },

  describeAction(action) {
    return action.reason || action.description || action.type || 'Aktion';
  }
});
