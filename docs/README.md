# HopNet Documentation

## Overview

This directory contains all technical documentation for HopNet, a distributed filesystem designed for everyone from enthusiast to enterprise.

## Quick Start

- **[System Overview](system-overview.md)** - High-level architecture, progress tracking, and development roadmap
- **Technical Specifications** - Detailed RFCs for each subsystem (see specs/ directory)

## Document Structure

The **[system-overview.md](system-overview.md)** serves as the primary dashboard for the entire project, containing:
- System component breakdown with progress tracking
- Current development focus and priorities
- Technology stack and development roadmap

The **specs/** directory contains detailed technical specifications for each major subsystem. The specs are also summarized in **[system-overview.md](system-overview.md)**.

## Progress Tracking

The documentation uses simple progress indicators across two levels:

**System Overview Level** (high-level component status):
- `[x]` = Complete
- `[~]` = In Progress  
- `[ ]` = Not Started
- `[!]` = Blocked

**RFC Level** (detailed implementation phases):
- Phase headers use the same bracket notation for overall phase status
- Individual tasks within phases track granular progress
- Update both levels when making implementation changes

## Navigation

For **product overview** and **development priorities**: Start with [system-overview.md](system-overview.md)

For **implementation details**: Refer to individual RFCs in the specs/ directory

For **current development status**: Check progress indicators in the system overview

## Contributing

When adding new features or making architectural changes:
1. Update progress indicators in system-overview.md (system component level)
2. Update progress indicators in relevant RFCs (implementation phase level)
3. Create or update RFCs in specs/ as needed
4. Link between documents for easy navigation

**Progress Sync Requirements**: Changes to code should update both system overview status AND corresponding RFC implementation phase status to maintain documentation accuracy.