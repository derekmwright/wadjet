package mcp

import _ "embed"

//go:embed docs/alerts.md
var alertsDocMD string

// alertsDocURI is the MCP resource URI that serves the alerts doc.
const alertsDocURI = "wadjet://docs/alerts"
