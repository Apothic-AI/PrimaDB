// @ts-check

const config = {
  title: "PrimaDB",
  tagline: "Local-first graph database with relay and mesh replication",
  url: "https://apothic-ai.github.io",
  baseUrl: "/PrimaDB/",
  organizationName: "Apothic-AI",
  projectName: "PrimaDB",
  trailingSlash: false,
  onBrokenLinks: "throw",
  i18n: {
    defaultLocale: "en",
    locales: ["en"],
  },
  markdown: {
    hooks: {
      onBrokenMarkdownLinks: "throw",
    },
  },
  presets: [
    [
      "classic",
      {
        docs: {
          path: "../docs",
          routeBasePath: "/",
          sidebarPath: require.resolve("./sidebars.js"),
          editUrl: "https://github.com/Apothic-AI/PrimaDB/tree/master/",
          showLastUpdateAuthor: false,
          showLastUpdateTime: false,
        },
        blog: false,
        theme: {
          customCss: require.resolve("./src/css/custom.css"),
        },
      },
    ],
  ],
  themeConfig: {
    navbar: {
      title: "PrimaDB",
      items: [
        {
          type: "docSidebar",
          sidebarId: "docs",
          position: "left",
          label: "Documentation",
        },
        {
          href: "https://github.com/Apothic-AI/PrimaDB",
          label: "GitHub",
          position: "right",
        },
      ],
    },
    footer: {
      style: "dark",
      links: [
        {
          title: "Docs",
          items: [
            { label: "Overview", to: "/" },
            { label: "Getting Started", to: "/category/getting-started" },
            { label: "Examples", to: "/category/examples" },
          ],
        },
        {
          title: "SDKs",
          items: [
            { label: "TypeScript", to: "/sdk/typescript" },
            { label: "Node", to: "/sdk/node" },
            { label: "Python", to: "/sdk/python" },
          ],
        },
        {
          title: "Project",
          items: [
            { label: "GitHub", href: "https://github.com/Apothic-AI/PrimaDB" },
            { label: "Examples", href: "https://github.com/Apothic-AI/PrimaDB/tree/master/examples" },
          ],
        },
      ],
      copyright: `Copyright © ${new Date().getFullYear()} PrimaDB contributors.`,
    },
    docs: {
      sidebar: {
        hideable: true,
      },
    },
    prism: {
      additionalLanguages: ["rust", "bash", "json", "python", "typescript"],
    },
  },
};

module.exports = config;
