'use strict';

module.exports = () => ({
  async withTimeout(promise, timeoutMs, label) {
    let timeoutId;
    const timeoutPromise = new Promise((_, reject) => {
      timeoutId = setTimeout(() => {
        const error = new Error(`${label} timeout after ${timeoutMs} ms`);
        error.code = 'ETIMEDOUT';
        reject(error);
      }, timeoutMs);
    });

    try {
      return await Promise.race([promise, timeoutPromise]);
    } finally {
      if (timeoutId) {
        clearTimeout(timeoutId);
      }
    }
  },

  async refineActionsWithGpt(context, actions) {
    if (!this.openaiClient || actions.length === 0) {
      return actions;
    }
    if (this.isHistoryEnabled() && !context?.decisionBasis?.history) {
      this.log.warn('History enabled but decision basis missing - skipping GPT refinement.');
      return actions;
    }

    const payload = {
      context: {
        summary: context?.summary,
        live: {
          energy: context?.live?.energy || [],
          pv: context?.live?.pv || [],
          water: context?.live?.water || [],
          temperature: context?.live?.temperature || []
        },
        decisionBasis: context?.decisionBasis || {}
      },
      actions: actions.map((action) => ({
        id: action.id,
        category: action.category,
        type: action.type,
        priority: action.priority,
        title: action.title,
        description: action.description,
        reason: action.reason,
        learningKey: action.learningKey
      }))
    };

    const prompt =
      'Du bist ein Assistent für einen Haus-Autopiloten. Verfeinere ausschließlich die Wortwahl ' +
      'der Aktionsfelder title, description und reason. Erfinde keine neuen Aktionen, ändere keine IDs, ' +
      'Kategorien, Prioritäten, Status oder Learning Keys. Gib ausschließlich ein JSON-Array zurück, in dem ' +
      'jede Zeile ein Objekt mit id, title, description, reason enthält.\n\n' +
      JSON.stringify(payload, null, 2);

    try {
      if (this.config.debug) {
        this.log.info('[DEBUG] GPT action refinement request sent');
      }
      const response = await this.withTimeout(
        this.openaiClient.responses.create({
          model: this.config.model || 'gpt-4o',
          input: [
            {
              role: 'user',
              content: [
                {
                  type: 'input_text',
                  text: prompt
                }
              ]
            }
          ]
        }),
        15000,
        'GPT action refinement'
      );

      const outputText = response.output_text || this.extractOutputText(response);
      if (this.config.debug) {
        this.log.info(`[DEBUG] GPT refinement request: ${this.trimLog(prompt)}`);
        this.log.info(`[DEBUG] GPT refinement response: ${this.trimLog(outputText || '')}`);
      }

      const refinements = this.parseJsonArray(outputText);
      if (!Array.isArray(refinements)) {
        return actions;
      }

      const refinementMap = new Map();
      for (const entry of refinements) {
        if (!entry || typeof entry.id !== 'string') {
          continue;
        }
        refinementMap.set(entry.id, entry);
      }

      const updatedActions = actions.map((action) => {
        const refinement = refinementMap.get(action.id);
        if (!refinement) {
          return action;
        }
        return {
          ...action,
          title: typeof refinement.title === 'string' ? refinement.title : action.title,
          description: typeof refinement.description === 'string' ? refinement.description : action.description,
          reason: typeof refinement.reason === 'string' ? refinement.reason : action.reason
        };
      });

      this.logDebug('GPT action refinement applied', updatedActions);
      return updatedActions;
    } catch (error) {
      this.log.warn(`GPT Aktionstext-Verfeinerung fehlgeschlagen: ${error.message}`);
      return actions;
    }
  },

  async buildGptSuggestedActions(context) {
    if (!this.openaiClient) {
      return [];
    }
    if (this.isHistoryEnabled() && !context?.decisionBasis?.history) {
      this.log.warn('History enabled but decision basis missing - skipping GPT suggestions.');
      return [];
    }

    const actionContext = this.buildActionContext(context);
    const payload = {
      context: {
        summary: context?.summary,
        live: {
          energy: context?.live?.energy || [],
          pv: context?.live?.pv || [],
          water: context?.live?.water || [],
          temperature: context?.live?.temperature || []
        },
        decisionBasis: context?.decisionBasis || {},
        learning: {
          feedback: context?.learning?.feedback || [],
          stats: context?.learning?.stats || {}
        }
      },
      schema: {
        fields: [
          'id',
          'category',
          'type',
          'target',
          'value',
          'unit',
          'priority',
          'reason',
          'requiresApproval',
          'learningKey'
        ]
      }
    };

    const prompt =
      'Du bist ein Assistent für einen Haus-Autopiloten. Schlage zusätzliche Aktionen vor, ' +
      'die auf den Live-Daten basieren. Gib ausschließlich ein JSON-Array zurück. Jede Aktion muss ' +
      'die Felder id, category (energy|heating|water|pv|safety), type, priority (low|medium|high), reason, ' +
      'requiresApproval (boolean) und learningKey enthalten. Optional target, value, unit. ' +
      'Keine Freitexte außerhalb des JSON.\n\n' +
      JSON.stringify(payload, null, 2);

    try {
      if (this.config.debug) {
        this.log.info('[DEBUG] GPT suggested actions request sent');
      }
      const response = await this.withTimeout(
        this.openaiClient.responses.create({
          model: this.config.model || 'gpt-4o',
          input: [
            {
              role: 'user',
              content: [
                {
                  type: 'input_text',
                  text: prompt
                }
              ]
            }
          ]
        }),
        15000,
        'GPT suggested actions'
      );

      const outputText = response.output_text || this.extractOutputText(response);
      if (this.config.debug) {
        this.log.info(`[DEBUG] GPT suggested actions response: ${this.trimLog(outputText || '')}`);
      }

      const suggestions = this.parseJsonArray(outputText);
      if (!Array.isArray(suggestions)) {
        return [];
      }

      const baseId = Date.now();
      let index = 1;
      const actions = [];
      for (const entry of suggestions) {
        const normalized = this.normalizeGptAction(entry, actionContext, baseId, index++);
        if (!normalized) {
          continue;
        }
        actions.push(normalized);
      }

      this.logDebug('GPT suggested actions derived', actions);
      return actions;
    } catch (error) {
      this.log.warn(`GPT Action-Vorschläge fehlgeschlagen: ${error.message}`);
      return [];
    }
  },

  async generateGptInsights(context, energySummary, recommendations) {
    if (!this.openaiClient) {
      return 'OpenAI not configured - GPT analysis skipped.';
    }
    if (this.isHistoryEnabled() && !context?.decisionBasis?.history) {
      this.log.warn('History enabled but decision basis missing - skipping GPT insights.');
      return 'GPT insights not available.';
    }

    const prompt =
      'Du bist ein Assistent für einen Haus-Autopiloten. ' +
      'Gib eine kurze, prägnante Zusammenfassung (max. 3 Sätze) mit den wichtigsten Beobachtungen ' +
      'zu Energie-, Wasser- und Temperaturdaten. Keine Aufzählungen. ' +
      'Consider previous approved and rejected actions to adapt recommendations.\n\n' +
      JSON.stringify(
        {
          context: {
            summary: energySummary,
            live: {
              energy: context.live.energy,
              water: context.live.water,
              temperature: context.live.temperature
            },
            recommendations,
            learning: {
              feedback: context.learning?.feedback || [],
              stats: context.learning?.stats || {}
            },
            decisionBasis: context?.decisionBasis || {}
          }
        },
        null,
        2
      );

    try {
      if (this.config.debug) {
        this.log.info('[DEBUG] GPT insights request sent');
      }
      const response = await this.withTimeout(
        this.openaiClient.responses.create({
          model: this.config.model || 'gpt-4o',
          input: [
            {
              role: 'user',
              content: [
                {
                  type: 'input_text',
                  text: prompt
                }
              ]
            }
          ]
        }),
        15000,
        'GPT insights'
      );

      const outputText = response.output_text || this.extractOutputText(response);
      if (this.config.debug) {
        this.log.info(`[DEBUG] GPT insights response: ${this.trimLog(outputText || '')}`);
      }
      return outputText && outputText.trim()
        ? outputText.trim()
        : 'GPT insights not available.';
    } catch (error) {
      this.log.warn(`GPT Insights fehlgeschlagen: ${error.message}`);
      return 'GPT insights not available.';
    }
  },

  extractOutputText(response) {
    if (!response || !response.output) {
      return '';
    }
    const texts = [];
    for (const item of response.output) {
      for (const content of item.content || []) {
        if (content.type === 'output_text') {
          texts.push(content.text);
        }
      }
    }
    return texts.join('\n');
  },

  parseJsonArray(text) {
    if (!text || typeof text !== 'string') {
      return null;
    }
    const match = text.match(/\[[\s\S]*\]/);
    if (!match) {
      return null;
    }
    try {
      return JSON.parse(match[0]);
    } catch (error) {
      this.handleError('GPT JSON konnte nicht geparst werden', error, true);
      return null;
    }
  },

  buildActionContext(context) {
    const timestamp = context?.timestamp || new Date().toISOString();
    const summary = context?.summary || {};
    return {
      timestamp,
      batterySoc: summary.batterySoc ?? null,
      houseConsumption: summary.houseConsumption ?? null,
      outsideTemp: this.getOutsideTemperature(context?.live?.temperature || []),
      pvPower: summary.pvPower ?? null
    };
  },

  normalizeGptAction(entry, actionContext, baseId, index) {
    if (!entry || typeof entry !== 'object') {
      return null;
    }
    const type = typeof entry.type === 'string' ? entry.type.trim() : '';
    if (!type) {
      return null;
    }
    const id = typeof entry.id === 'string' && entry.id.trim() ? entry.id : `${baseId}-${index}`;
    const category = this.normalizeActionCategory(entry.category);
    const priority = this.normalizeActionPriority(entry.priority);
    const requiresApproval = typeof entry.requiresApproval === 'boolean' ? entry.requiresApproval : true;
    const learningKey =
      typeof entry.learningKey === 'string' && entry.learningKey.trim()
        ? entry.learningKey
        : `${category}_${type}`;
    const value =
      typeof entry.value === 'number' || typeof entry.value === 'boolean' ? entry.value : undefined;
    const unit = typeof entry.unit === 'string' ? entry.unit : undefined;
    return {
      id,
      category,
      type,
      target: entry.target,
      value,
      unit,
      priority,
      source: 'gpt',
      reason: typeof entry.reason === 'string' ? entry.reason : 'GPT Vorschlag',
      context: actionContext,
      requiresApproval,
      status: 'proposed',
      decision: null,
      timestamps: this.buildActionTimestamps(),
      learningKey,
      title: typeof entry.title === 'string' ? entry.title : this.formatActionTitle(type),
      description: typeof entry.description === 'string' ? entry.description : undefined
    };
  },

  trimLog(text) {
    if (!text) {
      return '';
    }
    return text.length > this.constants.GPT_LOG_TRIM ? `${text.slice(0, this.constants.GPT_LOG_TRIM)}...` : text;
  }
});
