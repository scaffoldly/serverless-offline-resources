/* eslint-disable @typescript-eslint/no-var-requires,no-undef */
const path = require("path");

// eslint-disable-next-line no-undef
module.exports = {
  // CLI Bundling
  target: "node",

  // bundling mode
  mode: "production",

  // entry files
  entry: "./src/index.ts",

  // output bundles (location)
  output: {
    path: path.resolve(__dirname),
    filename: "index.js",
    library: "ServerlessOfflineResources",
    libraryTarget: "var",
  },

  // file resolutions
  resolve: {
    extensions: [".ts", ".js"],
  },

  // loaders
  module: {
    rules: [
      {
        test: /\.tsx?/,
        use: "ts-loader",
        exclude: /node_modules/,
      },
    ],
  },

  devtool: "source-map",
};
