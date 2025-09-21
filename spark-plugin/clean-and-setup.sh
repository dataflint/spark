#!/bin/bash

# Clean and setup script for DataFlint Spark plugin development
# This resolves cross-version dependency conflicts in IntelliJ IDEA

echo "🧹 Cleaning local ivy cache and build artifacts..."

# Remove all local dataflint artifacts
rm -rf ~/.ivy2/local/io.dataflint/

# Clean all target directories
find . -name "target" -type d -exec rm -rf {} + 2>/dev/null || true

echo "📦 Publishing required versions..."

# Publish Scala 2.12 version for Spark 3.x projects
echo "Publishing Scala 2.12 version..."
sbt plugin/publishLocal

# Publish Scala 2.13 version for Spark 4.x projects  
echo "Publishing Scala 2.13 version..."
sbt ++2.13.16 plugin/publishLocal

echo "🔨 Building fat JARs..."

# Build Spark 3 fat JAR
echo "Building Spark 3 fat JAR..."
sbt pluginspark3/assembly

# Build Spark 4 fat JAR
echo "Building Spark 4 fat JAR..."
sbt ++2.13.16 pluginspark4/assembly

echo "✅ Setup complete!"
echo ""
echo "📋 Next steps:"
echo "1. Refresh your IntelliJ IDEA project (File -> Reload Gradle Project or similar)"
echo "2. If you still get conflicts, try: File -> Invalidate Caches and Restart"
echo ""
echo "📦 Fat JARs created:"
echo "- Spark 3.x: pluginspark3/target/scala-2.12/dataflint-spark3_2.12-0.6.0-SNAPSHOT.jar"
echo "- Spark 4.x: pluginspark4/target/scala-2.13/dataflint-spark4_2.13-0.6.0-SNAPSHOT.jar"

