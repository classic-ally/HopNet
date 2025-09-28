// Shared content for security education components

export const SECURITY_OVERVIEW = {
  title: "Understanding HopNet's Security Design",
  bgColor: "surface1/50" as const,
  headerBgColor: "surface1" as const,
  borderColor: "border-surface1/30" as const,
  textColor: "primary" as const,
  headerIcon: "information" as const,
  paragraphs: [
    "HopNet balances uptime and security by providing strong file encryption regardless of network size, while consensus protections scale automatically with your infrastructure. In private networks, maintaining operations is typically more critical than theoretical coordinated attack tolerance.",
    "HopNet automatically strengthens consensus guarantees as you add more machines - no manual configuration required. The system selects consensus participants and adjusts protection levels based on your deployment size, ensuring optimal security for your current infrastructure without requiring distributed systems expertise.",
  ]
} as const;

export const ALWAYS_PROTECTED_BOX_ITEMS = [
  {
    title: "Files are encrypted individually",
    subtitles: [
      "Each file gets its own encryption key - only people you explicitly grant access can decrypt files, so compromising one doesn't affect others"
    ],
    icon: "document-security"
  },
  {
    title: "File and folder names are encrypted",
    subtitles: [
      "Path names are encrypted keeping your folder structure private",
      "Each user has unique path encryption keys"
    ],
    icon: "folder-off"
  },
  {
    title: "Keys only exist on your device",
    subtitles: [
      "Even coordinated network attacks can't read your files",
      "Network administrators cannot impersonate you or access your data"
    ],
    icon: "locked"
  },
  {
    title: "File tampering is always detected",
    subtitles: [
      "Cryptographic signatures make unauthorized file changes immediately detectable"
    ],
    icon: "data-check"
  }
] as const;

// Core protection items available at different consensus levels
export const CORE_PROTECTIONS = {
  CRASH: [
    {
      title: "Individual node failures",
      subtitles: ["Network continues operating when individual computers crash, lose power, or become unreachable"],
      icon: "warning"
    },
    {
      title: "Maintenance availability",
      subtitles: ["System remains online during planned maintenance, updates, or configuration changes"],
      icon: "tools"
    }
  ],
  ANOMALY: [
    {
      title: "Coordinated malicious behavior (up to 1/3)",
      subtitles: ["Prevents validator conspiracies from manipulating consensus decisions or approving invalid operations"],
      icon: "security"
    },
    {
      title: "Selective service denial attempts",
      subtitles: ["Malicious validators cannot block legitimate transactions from specific users or organizations"],
      icon: "block-storage"
    },
    {
      title: "Unfair resource allocation attempts",
      subtitles: ["Prevents biased storage quota assignments or preferential treatment in resource distribution"],
      icon: "scales"
    },
    {
      title: "History rewriting attacks",
      subtitles: ["Cryptographically prevents altering previously committed transactions or file operations"],
      icon: "version"
    }
  ]
} as const;

// Consensus mode configurations
export const CONSENSUS_MODES = {
  setup: {
    name: "Setup Mode",
    subtitle: "Development Only",
    description: "Minimal protection for development, testing, or initial network setup. Add validators immediately for production.",
    color: "red" as const,
    icon: "warning-multiple",
    bgColor: "red/10" as const,
    borderColor: "border-red/30" as const,
    protectedItems: [],
    notProtectedItems: [
      {
        title: "Any node failures (single point of failure)",
        subtitles: ["Entire network goes offline if the single validator computer fails"],
        icon: "warning-alt"
      },
      {
        title: "Network partitions or connectivity issues",
        subtitles: ["Connection problems can make the entire storage system inaccessible"],
        icon: "network-3"
      },
      {
        title: "Coordinated malicious behavior",
        subtitles: ["Single validator controls all consensus decisions and can manipulate the system"],
        icon: "user-multiple"
      },
      {
        title: "Selective service denial",
        subtitles: ["Validator can arbitrarily block operations from specific users"],
        icon: "block-storage"
      },
      {
        title: "Unfair resource allocation",
        subtitles: ["No protection against biased storage quotas or resource distribution"],
        icon: "scales"
      },
      {
        title: "History rewriting attacks",
        subtitles: ["Single validator can alter or delete previously committed operations"],
        icon: "version"
      }
    ],
    additionalContext: [
      {
        type: "warning" as const,
        title: "Immediate Action Required:",
        content: "Add more validators immediately for production use. Even 3 validators provides significant resilience improvements over setup mode.",
        bgColor: "yellow/10" as const,
        borderColor: "border-yellow/30" as const,
        textColor: "yellow" as const
      }
    ]
  },
  crash: {
    name: "Crash Protection",
    subtitle: "Operational Reliability",
    description: "Prioritizes keeping your network online and responsive. Suitable for private networks with trusted operators.",
    color: "yellow" as const,
    icon: "checkmark",
    bgColor: "yellow/10" as const,
    borderColor: "border-yellow/30" as const,
    protectedItems: CORE_PROTECTIONS.CRASH,
    notProtectedItems: [
      {
        title: "Coordinated malicious behavior by majority",
        subtitles: ["Validator majority conspiracy (50%+1) can issue illegitimate operations"],
        icon: "user-multiple"
      },
      {
        title: "Selective service denial by validators",
        subtitles: ["Validator majority conspiracy can block legitimate transactions"],
        icon: "block-storage"
      },
      {
        title: "Unfair resource allocation decisions",
        subtitles: ["Majority can make biased storage quota or resource distribution decisions"],
        icon: "scales"
      },
      {
        title: "History rewriting by majority",
        subtitles: ["Validator majority can potentially alter previously committed transactions"],
        icon: "version"
      }
    ],
    additionalContext: [
      {
        type: "info" as const,
        title: "Risk Context for Private Networks:",
        content: "In networks where you control or trust validator operators, coordination attacks are primarily theoretical. Cryptographic protections prevent transaction forgery - malicious validators can only make biased decisions about legitimate operations.",
        bgColor: "surface1" as const,
        borderColor: "" as const,
        textColor: "primary" as const
      },
      {
        type: "success" as const,
        title: "Consider upgrading when:",
        content: [
          "Multiple organizations with competing interests operate validators",
          "Regulatory compliance requires Byzantine fault tolerance",
          "High-value transactions where coordination attacks become economically motivated",
          "Mathematical guarantees of fairness are required"
        ],
        bgColor: "green/10" as const,
        borderColor: "border-green/30" as const,
        textColor: "green" as const
      }
    ]
  },
  anomaly: {
    name: "Crash + Anomaly Protection",
    subtitle: "Maximum Security",
    description: "Mathematical guarantees against failures and malicious behavior. For multi-organization networks or high-security requirements.",
    color: "green" as const,
    icon: "locked",
    bgColor: "green/10" as const,
    borderColor: "border-green/30" as const,
    protectedItems: [...CORE_PROTECTIONS.CRASH, ...CORE_PROTECTIONS.ANOMALY],
    notProtectedItems: [
      {
        title: "All major threats are protected against",
        subtitles: ["Byzantine fault tolerance provides mathematical guarantees against coordination attacks"],
        icon: "checkmark-filled"
      }
    ],
    additionalContext: [
      {
        type: "info" as const,
        title: "Byzantine Fault Tolerance:",
        content: "Uses the proven 2/3+1 consensus threshold that guarantees safety and liveness even when up to 1/3 of validators are compromised, offline, or acting maliciously. Provides the strongest possible guarantees in distributed systems.",
        bgColor: "surface1" as const,
        borderColor: "" as const,
        textColor: "primary" as const
      },
      {
        type: "info" as const,
        title: "Best For:",
        content: [
          "Multi-organization deployments with competing interests",
          "Regulatory environments requiring provable security guarantees",
          "High-value data where coordination attacks are economically motivated",
          "Long-term production deployments requiring maximum resilience"
        ],
        bgColor: "blue/10" as const,
        borderColor: "border-blue/30" as const,
        textColor: "blue" as const
      }
    ]
  }
} as const;

// Story configurations using the shared content
export const STORY_CONFIGS = {
  ITEMS_MODE: {
    title: "Protected Against",
    bgColor: "green/5" as const,
    borderColor: "border-green/30" as const,
    textColor: "green" as const,
    headerIcon: "checkmark-filled" as const,
    items: CORE_PROTECTIONS.CRASH
  },
  WARNING_STYLE: {
    title: "Not Protected Against",
    bgColor: "red/5" as const,
    borderColor: "border-red/30" as const,
    textColor: "red" as const,
    headerIcon: "warning-filled" as const,
    items: CONSENSUS_MODES.crash.notProtectedItems
  },
  INFO_STYLE: {
    title: "Always Protected",
    bgColor: "blue/5" as const,
    borderColor: "border-blue/30" as const,
    textColor: "blue" as const,
    headerIcon: "locked" as const,
    items: [ALWAYS_PROTECTED_BOX_ITEMS[0]] // Just the first item for the story
  }
} as const;