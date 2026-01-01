"use strict";

/**
 * Hauptdatei des Adapters.
 * Hier werden nur Lifecycle-Handler verdrahtet und die Module zusammengesetzt.
 */

const utils = require("@iobroker/adapter-core");
const createConfig = require("./lib/config");
const createState = require("./lib/state");
const createDiscovery = require("./lib/discovery");
const createLiveContext = require("./lib/liveContext");
const createHistory = require("./lib/history");
const createStats = require("./lib/stats");
const createRules = require("./lib/rules");
const createActions = require("./lib/actions");
const createReport = require("./lib/report");
const createTelegram = require("./lib/telegram");
const createGpt = require("./lib/gpt");
const createScheduler = require("./lib/scheduler");

/**
 * Adapter-Factory.
 */
function startAdapter(options) {
  const adapter = new utils.Adapter({
    ...options,
    name: "ai-autopilot",
  });

  const config = createConfig(adapter);
  const state = createState(adapter);
  const discovery = createDiscovery(adapter);
  const liveContext = createLiveContext(adapter);
  const history = createHistory(adapter);
  const stats = createStats(adapter);
  const rules = createRules(adapter);
  const actions = createActions(adapter);
  const report = createReport(adapter);
  const telegram = createTelegram(adapter);
  const gpt = createGpt(adapter);
  const scheduler = createScheduler(adapter);

  let analysisRunning = false;

  /**
   * Führt die Analyse sicher und mit Sperre aus.
   */
  async function runAnalysisWithLock(trigger) {
    if (analysisRunning) {
      adapter.log.info(`Analyse bereits aktiv, Trigger '${trigger}' wird ignoriert.`);
      return;
    }
    analysisRunning = true;
    adapter.log.info(`Starte Analyse (Trigger: ${trigger}).`);
    try {
      await runAnalysis();
      await state.setMeta("lastRun", new Date().toISOString());
    } catch (error) {
      adapter.log.error(`Analyse fehlgeschlagen: ${error.message}`);
      await state.setInfo("lastError", String(error.message || error));
    } finally {
      analysisRunning = false;
    }
  }

  /**
   * Gesamter Analyse-Workflow gemäß Spezifikation.
   */
  async function runAnalysis() {
    const normalizedConfig = config.normalize();

    if (!normalizedConfig.dataPoints.length) {
      adapter.log.info("Keine Datenpunkte konfiguriert. Analyse läuft im Leerlauf.");
      await report.persistEmpty();
      return;
    }

    const live = await liveContext.collect(normalizedConfig);
    const historyData = await history.collect(normalizedConfig, live);
    const computedStats = stats.compute(normalizedConfig, live, historyData);
    const deviations = rules.detectDeviations(normalizedConfig, live, historyData, computedStats);
    const actionList = actions.build(normalizedConfig, computedStats, deviations);
    const enrichedActions = await gpt.enrichActions(normalizedConfig, actionList, computedStats);
    const finalReport = report.build(normalizedConfig, live, historyData, computedStats, enrichedActions);

    await report.persist(finalReport);
  }

  adapter.on("ready", async () => {
    try {
      await state.ensureStates();
      await state.setInfo("connection", true);
      await state.setInfo("lastError", "");

      const normalizedConfig = config.normalize();
      await telegram.setup(normalizedConfig);
      await scheduler.start(normalizedConfig, () => runAnalysisWithLock("scheduler"));

      adapter.subscribeStates("control.run");
      adapter.log.info("Adapter ist bereit.");
    } catch (error) {
      adapter.log.error(`onReady Fehler: ${error.message}`);
      await state.setInfo("connection", false);
    }
  });

  adapter.on("stateChange", async (id, stateObj) => {
    if (!stateObj || stateObj.ack) {
      return;
    }

    if (id.endsWith("control.run") && stateObj.val === true) {
      await adapter.setStateAsync("control.run", false, true);
      await runAnalysisWithLock("control.run");
    }
  });

  adapter.on("message", async (msg) => {
    if (!msg || !msg.command) {
      return;
    }

    if (msg.command === "runDiscovery") {
      adapter.log.info("Starte Discovery auf Anforderung der Admin-Oberfläche.");
      try {
        const result = await discovery.runDiscovery();
        await adapter.extendForeignObjectAsync(adapter.namespace, {
          native: {
            discoveryCandidates: result,
          },
        });
        adapter.sendTo(msg.from, msg.command, { ok: true, count: result.length }, msg.callback);
      } catch (error) {
        adapter.log.warn(`Discovery fehlgeschlagen: ${error.message}`);
        adapter.sendTo(msg.from, msg.command, { ok: false, error: error.message }, msg.callback);
      }
      return;
    }

    if (msg.command === "telegramAction") {
      await telegram.handleAction(msg);
    }
  });

  adapter.on("unload", async (callback) => {
    try {
      await scheduler.stop();
      await telegram.stop();
      await state.setInfo("connection", false);
      callback();
    } catch (error) {
      adapter.log.error(`onUnload Fehler: ${error.message}`);
      callback();
    }
  });

  return adapter;
}

if (require.main !== module) {
  module.exports = startAdapter;
} else {
  startAdapter();
}
