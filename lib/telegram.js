'use strict';

module.exports = () => ({
  async sendTelegramMessage(text, options = {}) {
    if (!this.config.telegram.enabled || !this.config.telegram.instance) {
      if (this.config.debug && this.config.telegram.enabled) {
        this.log.info('[DEBUG] Telegram enabled but instance missing.');
      }
      return;
    }

    if (!this.config.telegram.chatId) {
      this.log.warn('Telegram chatId fehlt. Nachricht wird ohne chatId gesendet.');
    }

    const { includeKeyboard = false, parseMode, actions = [] } = options;

    try {
      if (this.config.debug) {
        this.log.info(`[DEBUG] Telegram send: ${text}`);
      }
      const payload = {
        text,
        chatId: this.config.telegram.chatId || undefined
      };
      if (parseMode) {
        payload.parse_mode = parseMode;
      }

      if (includeKeyboard) {
        payload.reply_markup = {
          inline_keyboard: this.buildTelegramKeyboard(actions)
        };
      }

      this.sendTo(this.config.telegram.instance, 'send', payload);
    } catch (error) {
      this.handleError('Telegram Versand fehlgeschlagen', error, true);
    }
  },

  buildTelegramKeyboard(actions) {
    const keyboard = [];
    for (const action of actions || []) {
      if (!action || !action.id) {
        continue;
      }
      keyboard.push([
        { text: '✅ Freigeben', callback_data: `action:${action.id}:approve` },
        { text: '❌ Ablehnen', callback_data: `action:${action.id}:reject` },
        { text: '✏️ Ändern', callback_data: `action:${action.id}:modify` }
      ]);
    }
    return keyboard.length > 0 ? keyboard : [];
  },

  async handleTelegramCallback(input = {}) {
    const callbackData = typeof input === 'string' ? input : input?.callbackData;
    const payload = typeof input === 'object' ? input?.payload : undefined;
    if (this.config.debug) {
      this.log.info(`[DEBUG] Telegram callback received: ${callbackData}`);
    }

    const actionMatch = String(callbackData || '').match(
      /^action:(.+):(approve|reject|modify|approved|rejected|executed)$/
    );
    if (actionMatch) {
      const actionId = actionMatch[1];
      const actionCommand = actionMatch[2];
      await this.handleActionCallback(actionId, actionCommand, payload);
      return;
    }

    if (!this.pendingActions) {
      this.log.warn('Telegram callback received but no pending actions are available.');
      return;
    }

    if (callbackData === 'approve_all') {
      await this.finalizeApproval('approved');
      return;
    }

    if (callbackData === 'reject_all') {
      await this.finalizeApproval('rejected');
      return;
    }
  },

  async handleTelegramText(text) {
    if (this.awaitingTelegramInput) {
      this.awaitingTelegramInput = false;
      const actionId = this.pendingModifyActionId;
      this.pendingModifyActionId = null;
      this.log.info(`Änderungswunsch: ${text}`);
      if (this.config.debug) {
        this.log.info(`[DEBUG] Telegram modify text received: ${text}`);
      }
      if (actionId) {
        await this.storeActionLearningDecision(actionId, 'modified');
      } else if (this.pendingActions) {
        await this.storeLearningEntries(
          this.pendingActions.map((action) => this.buildLearningEntry(action, 'modified'))
        );
      }
      return;
    }

    if (this.pendingActions) {
      await this.processFeedback(text);
    }
  },

  async finalizeApproval(decision) {
    if (!this.pendingActions) {
      return;
    }

    if (decision === 'approved') {
      this.updateActionStatuses(this.pendingActions, 'approved');
      await this.executeActions(this.pendingActions);
      this.pendingActions = null;
      return;
    }

    if (decision === 'rejected') {
      this.updateActionStatuses(this.pendingActions, 'rejected');
      for (const action of this.pendingActions) {
        action.executionResult = { status: 'skipped', reason: 'rejected' };
      }
      await this.persistActions(this.pendingActions, 'Actions rejected');
      await this.storeFeedbackEntries(
        this.pendingActions.map((action) =>
          this.buildFeedbackEntry(action, 'rejected', action.executionResult)
        )
      );
      await this.storeLearningEntries(
        this.pendingActions.map((action) => this.buildLearningEntry(action, 'rejected'))
      );
      await this.storeLearningHistoryEntries(
        this.pendingActions.map((action) => this.buildLearningHistoryEntry(action, 'rejected'))
      );
      this.log.info('Aktionen wurden abgelehnt.');
      this.pendingActions = null;
    }
  },

  async handleActionCallback(actionId, actionCommand, payload) {
    const actions = await this.loadActionsFromState();
    if (actions.length === 0) {
      this.log.warn('Keine gespeicherten Aktionen für Telegram Callback vorhanden.');
      return;
    }

    const action = actions.find((entry) => entry && String(entry.id) === String(actionId));
    if (!action) {
      this.log.warn(`Aktion nicht gefunden für Telegram Callback: ${actionId}`);
      return;
    }

    const normalizedCommand = this.normalizeActionCommand(actionCommand);
    if (!normalizedCommand) {
      this.log.warn(`Unbekanntes Telegram Kommando: ${actionCommand}`);
      return;
    }

    if (normalizedCommand === 'modify') {
      this.applyActionDecision(action, 'modified');
      await this.persistActions(actions, 'Action modification requested');
      this.awaitingTelegramInput = true;
      this.pendingModifyActionId = actionId;
      await this.sendTelegramActionConfirmation(action, 'modified');
      await this.updateTelegramOriginalMessage(payload, '✏️ Modification requested');
      await this.storeActionLearningDecision(actionId, 'modified');
      return;
    }

    if (normalizedCommand === 'reject') {
      const transitioned = this.applyActionStatusTransition(action, 'rejected');
      if (!transitioned) {
        return;
      }
      action.executionResult = { status: 'skipped', reason: 'rejected' };
      await this.persistActions(actions, 'Action rejected');
      await this.sendTelegramActionConfirmation(action, 'rejected');
      await this.updateTelegramOriginalMessage(payload, '❌ Rejected');
      await this.storeFeedbackEntries([this.buildFeedbackEntry(action, 'rejected', action.executionResult)]);
      await this.persistLearningForDecision(action, 'rejected');
      this.updatePendingAction(action);
      return;
    }

    if (normalizedCommand === 'approve') {
      const transitioned = this.applyActionStatusTransition(action, 'approved');
      if (!transitioned) {
        return;
      }
      await this.executeAction(action);
      await this.persistActions(actions, 'Action approved');
      await this.sendTelegramActionConfirmation(action, 'approved');
      await this.updateTelegramOriginalMessage(payload, '✅ Approved');
      await this.storeFeedbackEntries([this.buildFeedbackEntry(action, 'approved', action.executionResult)]);
      await this.persistLearningForDecision(action, 'executed');
      this.updatePendingAction(action);
      return;
    }

    if (normalizedCommand === 'executed') {
      const transitioned = this.applyActionStatusTransition(action, 'executed');
      if (!transitioned) {
        return;
      }
      await this.persistActions(actions, 'Action executed');
      await this.sendTelegramActionConfirmation(action, 'executed');
      await this.updateTelegramOriginalMessage(payload, '✅ Executed');
      await this.storeFeedbackEntries([this.buildFeedbackEntry(action, 'approved', action.executionResult)]);
      await this.persistLearningForDecision(action, 'executed');
      this.updatePendingAction(action);
    }
  },

  normalizeActionCommand(actionCommand) {
    switch (String(actionCommand || '').toLowerCase()) {
      case 'approve':
      case 'approved':
        return 'approve';
      case 'reject':
      case 'rejected':
        return 'reject';
      case 'modify':
        return 'modify';
      case 'executed':
        return 'executed';
      default:
        return null;
    }
  },

  async sendTelegramActionConfirmation(action, decision) {
    const actionLabel = this.describeAction(action);
    const prefixMap = {
      approved: '✅ Approved',
      rejected: '❌ Rejected',
      modified: '✏️ Modification requested',
      executed: '✅ Executed'
    };
    const prefix = prefixMap[decision] || 'ℹ️ Update';
    const text = `${prefix}: ${actionLabel}`;
    await this.sendTelegramMessage(text);
  },

  async updateTelegramOriginalMessage(payload, updateLabel) {
    if (!payload || !payload.message || !payload.message.message_id) {
      return;
    }
    if (!this.config.telegram.enabled || !this.config.telegram.instance) {
      return;
    }
    const messageId = payload.message.message_id;
    const chatId = payload.message.chat?.id || this.config.telegram.chatId;
    const originalText = payload.message.text || '';
    if (!originalText) {
      return;
    }
    const updatedText = `${originalText}\n\n${updateLabel}`;
    try {
      this.sendTo(this.config.telegram.instance, 'editMessageText', {
        chatId,
        message_id: messageId,
        text: updatedText,
        parse_mode: 'Markdown'
      });
      this.logDebug('Telegram message updated', { messageId, updateLabel });
    } catch (error) {
      this.handleError('Telegram message update failed', error, true);
    }
  },

  buildApprovalMessage(actions, reportText) {
    const analysisLabel = this.buildAnalysisLabel(actions);
    const timestamp = new Date();
    const timeLabel = timestamp.toLocaleTimeString('de-DE', { hour: '2-digit', minute: '2-digit' });
    const dateLabel = timestamp.toLocaleDateString('de-DE');
    const modeLabel = this.config.mode || 'auto';
    const dryRunLabel = this.config.dryRun ? 'Ja' : 'Nein';
    const lines = [
      '🤖🏠 *AI-Autopilot – Entscheidung erforderlich*',
      '',
      `🕒 *Zeitstempel:* ${dateLabel} ${timeLabel}`,
      `🧪 *Dry-Run:* ${dryRunLabel}`,
      `⚡ *Analyse:* ${analysisLabel}`,
      `⚙️ *Modus:* ${modeLabel}`,
      '',
      '📝 *Zusammenfassung:*',
      '```',
      reportText || 'Keine Zusammenfassung verfügbar.',
      '```',
      '',
      '🔎 *Vorgeschlagene Maßnahmen:*',
      ''
    ];

    for (const action of actions) {
      lines.push(this.formatActionLine(action));
    }

    lines.push('', '_Bitte auswählen:_');
    return lines.join('\n');
  },

  formatActionLine(action) {
    const priority = this.normalizeActionPriority(action.priority);
    const priorityLabel = priority.toUpperCase();
    const categoryEmoji = this.getCategoryEmoji(action.category);
    const emoji = this.getPriorityEmoji(priority);
    const title = action.title || this.formatActionTitle(action.type);
    const detail = action.reason || action.description;
    const description = detail ? ` (${detail})` : '';
    return `- ${categoryEmoji} ${emoji} *${title}*${description} _[${priorityLabel}]_`;
  },

  getPriorityEmoji(priority) {
    switch (String(priority || '').toLowerCase()) {
      case 'high':
      case 'critical':
        return '🔥';
      case 'medium':
      case 'warn':
        return '⚠️';
      default:
        return 'ℹ️';
    }
  },

  getCategoryEmoji(category) {
    switch (String(category || '').toLowerCase()) {
      case 'energy':
        return '⚡';
      case 'heating':
        return '🔥';
      case 'water':
        return '💧';
      case 'pv':
        return '☀️';
      case 'safety':
        return '🛡️';
      default:
        return '📌';
    }
  },

  buildAnalysisLabel(actions) {
    const categories = new Set((actions || []).map((action) => action.category).filter(Boolean));
    const labelMap = {
      energy: 'Energie',
      heating: 'Heizung',
      water: 'Wasser',
      pv: 'PV',
      safety: 'Sicherheit'
    };
    const labels = Array.from(categories)
      .map((category) => labelMap[category] || category)
      .filter(Boolean);
    return labels.length > 0 ? labels.join(' & ') : 'System';
  }
});
