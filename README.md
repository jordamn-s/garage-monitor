# Garage Door Monitor

A data engineering portfolio project that captures garage door RF signals using
an RTL-SDR dongle, stores them in PostgreSQL, transforms them with dbt, and
visualizes them in a Tableau Public dashboard.

## Stack
- **Signal capture:** rtl_433 with custom OOK_PWM decoder
- **Database:** PostgreSQL
- **Transformation:** dbt Core
- **Visualization:** Tableau Public
- **OS:** Linux (Ubuntu on capture machine, Fedora on dev machine)

## Architecture
RTL-SDR → rtl_433 → Python listener → PostgreSQL → dbt → Tableau Public

## Status
- Phase 1 (Capture + Storage): Complete
- Phase 2 (dbt Transformations): In progress
- Phase 3 (Tableau Dashboard): Planned
