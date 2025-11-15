package main

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type CompressionResult struct {
	Algorithm        string
	OriginalSize     int64
	CompressedSize   int64
	ReductionPercent float64
	CompressionRatio float64
	InsertTime       time.Duration
}

type CompressionDemo struct {
	client *mongo.Client
	ctx    context.Context
}

func (d *CompressionDemo) generateLargeDocument() primitive.D {
	// Create a ~4.7MB document with mixed content for realistic compression testing
	repetitiveText := strings.Repeat("This is highly compressible repetitive text pattern. ", 50000)

	// Create large array of structured documents
	items := make([]primitive.M, 5000)
	for i := 0; i < 5000; i++ {
		items[i] = primitive.M{
			"id":          i,
			"name":        fmt.Sprintf("Product_Item_Number_%d", i),
			"description": "This is a repeated item description that compresses efficiently with MongoDB compression algorithms",
			"price":       float64(i) * 1.99,
			"metadata": primitive.M{
				"tags":       []string{"electronics", "home", "kitchen", "premium"},
				"categories": []string{"main", "featured", "bestseller"},
				"features":   []string{"wireless", "bluetooth", "rechargeable", "smart"},
			},
			"reviews": primitive.M{
				"average_rating": 4.5,
				"total_reviews":  150,
				"stars":          []int{100, 200, 300, 250, 150},
			},
		}
	}

	// Generate some binary-like data (less compressible)
	binaryData := make([]byte, 300000)
	for i := range binaryData {
		binaryData[i] = byte(i % 256)
	}

	doc := primitive.D{
		{Key: "_id", Value: primitive.NewObjectID()},
		{Key: "description", Value: "Large 4.7MB document for MongoDB compression testing"},
		{Key: "timestamp", Value: time.Now()},
		{Key: "version", Value: "1.0"},
		{Key: "repetitive_text", Value: repetitiveText},
		{Key: "product_items", Value: items},
		{Key: "binary_data", Value: binaryData},
		{Key: "metadata", Value: primitive.M{
			"created_by":       "compression-demo",
			"document_type":    "performance_test",
			"size_category":    "4.7MB",
			"compression_test": true,
		}},
	}

	return doc
}

func (d *CompressionDemo) getDocumentSize(doc primitive.D) (int64, error) {
	marshal, err := bson.Marshal(doc)
	if err != nil {
		return 0, err
	}
	return int64(len(marshal)), nil
}

func (d *CompressionDemo) testCompression(algorithm, compressor string, doc primitive.D) (CompressionResult, error) {
	result := CompressionResult{Algorithm: algorithm}

	// Get original size
	originalSize, err := d.getDocumentSize(doc)
	if err != nil {
		return result, err
	}
	result.OriginalSize = originalSize

	// Create client with specific compression
	clientOptions := options.Client().
		ApplyURI("mongodb://localhost:27017").
		SetCompressors([]string{compressor}).
		SetAppName("compression-demo")

	client, err := mongo.Connect(d.ctx, clientOptions)
	if err != nil {
		return result, err
	}
	defer client.Disconnect(d.ctx)

	database := client.Database("compression_demo")
	collection := database.Collection(fmt.Sprintf("test_%s", strings.ToLower(algorithm)))

	// Clear previous data
	collection.Drop(d.ctx)

	// Test insert performance
	start := time.Now()
	_, err = collection.InsertOne(d.ctx, doc)
	if err != nil {
		return result, err
	}
	result.InsertTime = time.Since(start)

	// Get collection stats to determine storage size
	stats := database.RunCommand(d.ctx, primitive.D{
		{Key: "collStats", Value: collection.Name()},
	})

	var statsResult bson.M
	if err := stats.Decode(&statsResult); err != nil {
		return result, err
	}

	storageSize := statsResult["storageSize"].(int64)
	result.CompressedSize = storageSize
	result.ReductionPercent = (1 - float64(storageSize)/float64(originalSize)) * 100
	result.CompressionRatio = float64(originalSize) / float64(storageSize)

	return result, nil
}

func (d *CompressionDemo) runAllCompressionTests(doc primitive.D) ([]CompressionResult, error) {
	tests := []struct {
		name       string
		compressor string
	}{
		{"Snappy", "snappy"},
		{"Zlib", "zlib"},
		{"Zstd", "zstd"},
	}

	var results []CompressionResult

	for _, test := range tests {
		fmt.Printf("🧪 Testing %s compression...\n", test.name)
		result, err := d.testCompression(test.name, test.compressor, doc)
		if err != nil {
			return nil, fmt.Errorf("%s test failed: %v", test.name, err)
		}
		results = append(results, result)

		// Small delay between tests
		time.Sleep(100 * time.Millisecond)
	}

	return results, nil
}

func displayResults(results []CompressionResult, originalSizeMB float64) {
	blue := color.New(color.FgBlue).Add(color.Bold)
	green := color.New(color.FgGreen).Add(color.Bold)
	yellow := color.New(color.FgYellow).Add(color.Bold)
	red := color.New(color.FgRed).Add(color.Bold)
	cyan := color.New(color.FgCyan).Add(color.Bold)

	blue.Printf("\n🎯 COMPRESSION RESULTS FOR %.2fMB DOCUMENT\n", originalSizeMB)
	fmt.Println(strings.Repeat("═", 70))

	for _, result := range results {
		var colorizer *color.Color
		var emoji string

		switch result.Algorithm {
		case "Snappy":
			colorizer = yellow
			emoji = "🚀"
		case "Zlib":
			colorizer = green
			emoji = "📦"
		case "Zstd":
			colorizer = cyan
			emoji = "⚡"
		default:
			colorizer = red
			emoji = "❓"
		}

		colorizer.Printf("%s %s:\n", emoji, result.Algorithm)
		fmt.Printf("   📊 Original: %6.2f MB\n", float64(result.OriginalSize)/1024/1024)
		fmt.Printf("   💾 Compressed: %5.2f MB\n", float64(result.CompressedSize)/1024/1024)
		fmt.Printf("   📉 Reduction: %s%6.1f%%%s\n",
			getReductionColor(result.ReductionPercent),
			result.ReductionPercent,
			"\033[0m")
		fmt.Printf("   🎯 Ratio: %.2fx\n", result.CompressionRatio)
		fmt.Printf("   ⏱️  Insert Time: %v\n", result.InsertTime.Round(time.Millisecond))
		fmt.Println()
	}

	displayComparisonChart(results)
	displayPerformanceAnalysis(results)
}

func getReductionColor(reduction float64) string {
	switch {
	case reduction >= 50:
		return "\033[32m" // Green
	case reduction >= 30:
		return "\033[33m" // Yellow
	default:
		return "\033[31m" // Red
	}
}

func displayComparisonChart(results []CompressionResult) {
	red := color.New(color.FgRed)
	yellow := color.New(color.FgYellow)
	green := color.New(color.FgGreen)

	fmt.Println("📊 COMPRESSION PERFORMANCE COMPARISON:")
	fmt.Println(strings.Repeat("─", 60))

	for _, result := range results {
		bars := int(result.ReductionPercent / 2)
		bar := strings.Repeat("█", bars)

		var coloredBar string
		switch result.Algorithm {
		case "Snappy":
			coloredBar = yellow.Sprint(bar)
		case "Zlib":
			coloredBar = green.Sprint(bar)
		case "Zstd":
			coloredBar = green.Sprint(bar)
		default:
			coloredBar = red.Sprint(bar)
		}

		fmt.Printf("%-8s %s %5.1f%%\n",
			result.Algorithm,
			coloredBar,
			result.ReductionPercent)
	}
	fmt.Println(strings.Repeat("─", 60))
}
func displayPerformanceAnalysis(results []CompressionResult) {
	cyan := color.New(color.FgCyan).Add(color.Bold)
	magenta := color.New(color.FgMagenta).Add(color.Bold)

	cyan.Println("\n💡 PERFORMANCE ANALYSIS:")
	fmt.Println(strings.Repeat("─", 50))

	// Find best compression
	bestCompression := results[0]
	fastest := results[0]

	for _, result := range results {
		if result.ReductionPercent > bestCompression.ReductionPercent {
			bestCompression = result
		}
		if result.InsertTime < fastest.InsertTime {
			fastest = result
		}
	}

	fmt.Printf("🏆 Best Compression: %s (%.1f%% reduction)\n",
		bestCompression.Algorithm, bestCompression.ReductionPercent)
	fmt.Printf("⚡ Fastest Insert: %s (%v)\n",
		fastest.Algorithm, fastest.InsertTime.Round(time.Millisecond))

	// Network traffic simulation
	magenta.Println("\n🌐 NETWORK TRAFFIC SIMULATION (1,000 transfers):")
	originalSizeMB := float64(results[0].OriginalSize) / 1024 / 1024

	for _, result := range results {
		totalMB := float64(result.CompressedSize) / 1024 / 1024 * 1000
		savedMB := (originalSizeMB * 1000) - totalMB
		savingsPercent := (savedMB / (originalSizeMB * 1000)) * 100

		fmt.Printf("%-8s: %6.1f MB total (saves %5.1f MB, %4.1f%%)\n",
			result.Algorithm, totalMB, savedMB, savingsPercent)
	}

	// Cost analysis
	displayCostAnalysis(results, originalSizeMB)
}

func displayCostAnalysis(results []CompressionResult, originalSizeMB float64) {
	yellow := color.New(color.FgYellow).Add(color.Bold)

	yellow.Println("\n💸 CLOUD COST ANALYSIS (AWS Data Transfer $0.09/GB):")
	fmt.Println(strings.Repeat("─", 55))

	const costPerGB = 0.09
	const monthlyTransfers = 1000000 // 1 million transfers per month

	originalCostGB := (originalSizeMB * float64(monthlyTransfers)) / 1024
	originalCost := originalCostGB * costPerGB

	fmt.Printf("No Compression: $%.2f/month\n", originalCost)

	for _, result := range results {
		compressedSizeMB := float64(result.CompressedSize) / 1024 / 1024
		costGB := (compressedSizeMB * float64(monthlyTransfers)) / 1024
		cost := costGB * costPerGB
		savings := originalCost - cost

		fmt.Printf("%-8s: $%6.2f/month (saves $%5.2f, %.1f%% cost reduction)\n",
			result.Algorithm, cost, savings, (savings/originalCost)*100)
	}
}
func displayExpectedResults() {
	red := color.New(color.FgRed).Add(color.Bold)
	yellow := color.New(color.FgYellow).Add(color.Bold)
	green := color.New(color.FgGreen).Add(color.Bold)

	red.Println("\n🎯 EXPECTED RESULTS (Based on Your 4.7MB Document Test):")
	fmt.Println(strings.Repeat("═", 65))

	expected := map[string]struct {
		reduction float64
		sizeMB    float64
		color     *color.Color
		emoji     string
	}{
		"Snappy": {25, 3.53, yellow, "🚀"},
		"Zlib":   {52, 2.26, green, "📦"},
		"Zstd":   {53, 2.21, green, "⚡"},
	}

	for algo, data := range expected {
		data.color.Printf("%s %s:\n", data.emoji, algo)
		fmt.Printf("   • Reduction: %.1f%%\n", data.reduction)
		fmt.Printf("   • Final Size: %.2fMB\n", data.sizeMB)
		fmt.Printf("   • Bandwidth Saved: %.1f%%\n", data.reduction)
		fmt.Println()
	}

	green.Println("💡 KEY INSIGHTS:")
	fmt.Println("   • Zstd provides the best balance of compression and speed")
	fmt.Println("   • Zlib offers maximum compression but with higher CPU cost")
	fmt.Println("   • Snappy is fastest but provides less compression")
	fmt.Println("   • For 4.7MB documents, compression saves ~2.5MB per transfer!")
	fmt.Println(strings.Repeat("═", 65))
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Initialize demo
	demo := &CompressionDemo{ctx: ctx}

	// Connect to MongoDB
	client, err := mongo.Connect(ctx, options.Client().ApplyURI("mongodb://localhost:27017"))
	if err != nil {
		log.Fatalf("Failed to connect to MongoDB: %v", err)
	}
	defer client.Disconnect(ctx)
	demo.client = client

	fmt.Println("🚀 MongoDB Compression Demo - Golang")
	fmt.Println("Testing with 4.7MB document...")
	fmt.Println()

	// Generate test document
	fmt.Println("📄 Generating 4.7MB test document...")
	doc := demo.generateLargeDocument()

	originalSize, err := demo.getDocumentSize(doc)
	if err != nil {
		log.Fatalf("Failed to get document size: %v", err)
	}

	originalSizeMB := float64(originalSize) / 1024 / 1024
	fmt.Printf("✅ Generated document: %.2fMB\n\n", originalSizeMB)

	// Run compression tests
	fmt.Println("🧪 Running compression tests...")
	fmt.Println(strings.Repeat("─", 40))

	results, err := demo.runAllCompressionTests(doc)
	if err != nil {
		log.Fatalf("Compression tests failed: %v", err)
	}

	// Display results
	displayResults(results, originalSizeMB)

	// Show expected results based on user's findings
	displayExpectedResults()

	// Cleanup
	database := client.Database("compression_demo")
	database.Drop(ctx)

	fmt.Println("✅ Demo completed successfully!")
}
