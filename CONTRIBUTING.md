# Contributing to DataFlint

## Getting started with development

### Setup

Requirements:
1. Node v21.5.0
2. Java 8 or 11
3. Scala 2.12
4. SBT 1.3.13
5. IntelliJ IDEA with Scala and SBT plugins
6. Visual Studio Code

### Installation Steps

1. Clone the repository:
   ```
   git clone https://github.com/dataflint/spark.git
   cd spark
   ```

2. Set up the Spark Plugin:
   - Open the `spark-plugin` folder with IntelliJ IDEA
   - Ensure Scala and SBT plugins are installed in IntelliJ

3. Set up the UI:
   - Open the repository with Visual Studio Code
   - Install UI dependencies:
     ```
     cd spark-ui
     npm install
     ```

4. Build the UI for the plugin:
   ```
   cd spark-ui
   npm run deploy
   ```

5. (Optional) Install Local CORS Proxy (LCP) for local development:
   ```
   brew install lcp
   ```

### Running the Project

1. Run one of the examples in the `spark-examples-351` project using IntelliJ

2. Access the Spark UI:
   - Browse to `http://localhost:10000`
   - Open DataFlint successfully

### Live Frontend Development

For live frontend development, follow these steps:

1. Start the development server and proxy:
   ```
   cd spark-ui
   npm run start
   npm run proxy
   ```

2. Access the development UI:
   - Browse to `http://localhost:4000`
   - This should run the DataFlint UI with live reloading

## Contributing Guidelines

- Please ensure your code follows the project's coding standards
- Submit pull requests for review

Thank you for contributing to DataFlint!
