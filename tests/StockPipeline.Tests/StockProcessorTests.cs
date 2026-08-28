using StockPipeline.Web.Processing;
using Xunit;

namespace StockPipeline.Tests;

// This is the test the pipeline runs as the DEV -> QA gate (Part 6). If it
// fails, the promotion to QA stops — nothing broken reaches QA, let alone UAT
// or PROD.
public class StockProcessorTests
{
    [Theory]
    [InlineData(100.0, 105.0)]
    [InlineData(0.0, 5.0)]
    [InlineData(-5.0, 0.0)]
    [InlineData(94.37, 99.37)]
    public void ApplyAdjustment_AddsTheConstantToTheRawPrice(double rawPrice, double expectedProcessedPrice)
    {
        var actual = StockProcessor.ApplyAdjustment(rawPrice);

        Assert.Equal(expectedProcessedPrice, actual, precision: 5);
    }
}
