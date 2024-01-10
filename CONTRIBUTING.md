## Getting started with development

### Setup

Requirements:
1. Node v21.5.0
2. java 8 or 11
3. scala 2.12
4. sbt 1.3.13

#### (1) Clone the repository

```
git clone https://github.com/dataflint/spark.git
cd spark
```

#### (2) Install NPM requirements

```
cd ./spark-ui
npm install
```

#### (3) Install sbt requirements

```
cd ./spark-plugin
sbt
```

alternitivaly, open the spark-plugin project in intelij which will download the requirements for you)

### Development

For an IDE I recommned using VSCode for the web app and InteliJ for the spark plugin

To run a development environment, do the following steps:
1. Run a spark job on port 10000 that is running the DataFlint plugin, there is example spark jobs in the plugin projects
2. A CORS proxy or a chrome extension to disable proxy, you can run one using the command 'npm run proxy' from the spark-ui folder
3. React-script development server, can be run via 'npm run start'

