# ai-autopilot (ioBroker Adapter)

**ai-autopilot** ist ein experimenteller, aber modular aufgebauter ioBroker-Adapter zur
Analyse von Energie-, Wasser-, Temperatur- und weiteren Haushaltsdaten.  
Er kombiniert **Live-Daten** mit **historischen Daten** (InfluxDB / SQL) und erzeugt
strukturierte **Auswertungen, Statistiken und Handlungsempfehlungen**.

> ⚠️ **Status:**  
> Der Adapter befindet sich im Aufbau. Struktur, APIs und Konfiguration können sich
> noch ändern. Für produktive Systeme nur mit Vorsicht einsetzen.

---

## 🎯 Ziel des Adapters

Ziel ist ein **intelligenter Analyse- und Entscheidungs-Adapter**, der:

- **alle potenziellen Datenquellen automatisch erkennt**
  (Shelly, Sonoff, Homematic, Modbus, M-Bus, MQTT, Zigbee, …)
- diese **im Admin konfigurierbar** macht
- dem Nutzer erlaubt, **die Rolle jedes Datenpunkts festzulegen**
- **Live- und Historien-Daten** gemeinsam auswertet
- daraus **klare Statistiken und verständliche Berichte** erstellt
- **keine Aktoren automatisch schaltet**, sondern Empfehlungen liefert

---

## ✨ Kernfunktionen

### 🔍 Automatische Datenquellen-Erkennung
- Scan aller installierten ioBroker-Adapter
- Erkennung typischer Messrollen:
  - Leistung (W)
  - Energie (Wh / kWh)
  - Temperatur (°C)
  - Wasser (l / m³)
- Vorschläge werden angezeigt, aber **nicht automatisch aktiviert**

---

### ⚙️ Flexible Zuordnung im Admin (JSON-Config)

Für **jeden Datenpunkt** kann festgelegt werden:

- ✅ Aktiv / Inaktiv
- 🔌 Typ:
  - Gesamtverbrauch
  - Einzelverbraucher
  - Stromquelle (z. B. PV)
  - Netzbezug / Einspeisung
  - Batterie
  - Wallbox / EV
  - Wasser / Leckage
  - Temperatur / Raum / Außen
- 📊 Rolle für Auswertung
- 📈 Optionaler Tages- oder Zählerwert

Alles ist **erweiterbar**, eigene Datenpunkte können jederzeit ergänzt werden.

---

### 📊 Live- & Historien-Auswertung

- Live-Daten über `getForeignStateAsync`
- Historische Daten:
  - InfluxDB
  - SQL / MySQL
- Automatische Prüfung:
  - Ist ein History-Adapter installiert?
  - Sind für den Datenpunkt Daten vorhanden?
- Berechnung u. a.:
  - Durchschnitt
  - Min / Max
  - Tag / Nacht-Baseline
  - Trends
  - Abweichungen

---

### 🧠 Intelligenz-Ebene

- Zusammenfassung des aktuellen Zustands
- Erkennung von Auffälligkeiten
- Ableitung von **Handlungsempfehlungen**
- Optional:
  - GPT / OpenAI zur Text- und Kontextverbesserung
  - rein beratend, keine Pflicht

---

### 📬 Telegram (optional)

- Versand von:
  - Analyse-Berichten
  - Tageszusammenfassungen
  - Handlungsvorschlägen
- Inline-Buttons:
  - ✅ Freigeben
  - ❌ Ablehnen
  - ✏️ Ändern
- Adapter funktioniert **vollständig ohne Telegram**

---

### ⏱ Zeitgesteuerte Berichte (optional)

- Tägliche Reports
- Uhrzeit frei konfigurierbar
- Zeitzonen-Unterstützung
- Nur Auswertung, keine Schaltaktionen

---

## 🧱 Architektur & Code-Struktur

Der Adapter ist **konsequent modular aufgebaut**:
