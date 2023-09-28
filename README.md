# DataFlint spark

DataFlint is a observability and visibility tool for Apache Spark

## build instructions

1. `cd spark-ui`
2. `npm run build && npm run deploy`
3. `cd ../spark-plugin`
4. `sbt package`

## dev instruction
1. run the shakespeer example
2. `cd spark-ui`
3. `npm run start`
4. goto localhost:3000 for the dev UI and use a CORS extention to solve the CORS errors