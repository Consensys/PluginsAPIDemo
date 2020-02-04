package tech.pegasys.besu.plugins.demo;

import org.hyperledger.besu.plugin.BesuContext;
import org.hyperledger.besu.plugin.BesuPlugin;
import org.hyperledger.besu.plugin.data.PropagatedBlockContext;
import org.hyperledger.besu.plugin.data.Transaction;
import org.hyperledger.besu.plugin.services.BesuEvents;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.PicoCLIOptions;
import org.hyperledger.besu.plugin.services.metrics.Counter;
import org.hyperledger.besu.plugin.services.metrics.LabelledMetric;
import org.hyperledger.besu.plugin.services.metrics.MetricCategory;
import org.hyperledger.besu.plugin.services.metrics.MetricCategoryRegistry;

import java.util.Optional;

import com.google.auto.service.AutoService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine.Option;

// The AutoService annotation (when paired with the corresponding annotation processor) will
// automatically handle adding the relevant META-INF files so Besu will load this plugin.
@AutoService(BesuPlugin.class)
public class GasSpendingMetricsPlugin implements BesuPlugin {

  private static Logger LOG = LogManager.getLogger();

  private static String PLUGIN_NAME = "gas-spending-metrics";

  @Override
  public Optional<String> getName() {
    return Optional.of("Gas Spending Metrics");
  }

  private BesuContext context;

  @Override
  public void register(final BesuContext context) {
    LOG.info("Registering Gas Spending Metrics Plugin");
    this.context = context;

    context
        .getService(PicoCLIOptions.class)
        .ifPresentOrElse(
            this::createPicoCLIOptions, () -> LOG.error("Could not obtain PicoCLIOptionsService"));
    context
        .getService(MetricCategoryRegistry.class)
        .ifPresentOrElse(
            this::registerMetrics, () -> LOG.error("Could not obtain MetricCategoryRegistry"));
  }

  @Override
  public void start() {
    LOG.info("Starting Gas Spending Metrics Plugin");
    context
        .getService(MetricsSystem.class)
        .ifPresentOrElse(this::startMetrics, () -> LOG.error("Could not obtain MetricsSystem"));
    context
        .getService(BesuEvents.class)
        .ifPresentOrElse(this::startEvents, () -> LOG.error("Could not obtain BesuEvents"));
  }

  @Override
  public void stop() {
    LOG.info("Starting Gas Spending Metrics Plugin");
    context
        .getService(BesuEvents.class)
        .ifPresentOrElse(this::stopEvents, () -> LOG.error("Could not obtain BesuEvents"));
  }

  //
  // CLI Options
  //

  // CLI names must be of the form "--plugin-<namespace>-...."
  @Option(names = "--plugin-gas-spending-metrics-name")
  public String metricName = "gas_spending_metrics";

  @Option(names = "--plugin-gas-spending-metrics-prefix")
  public String metricPrefix = "demo_";

  private void createPicoCLIOptions(final PicoCLIOptions picoCLIOptions) {
    picoCLIOptions.addPicoCLIOptions(PLUGIN_NAME, this);
  }

  //
  // Metrics Category Registration
  //

  private final MetricCategory metricCategory =
      new MetricCategory() {
        @Override
        public String getName() {
          return metricName;
        }

        @Override
        public Optional<String> getApplicationPrefix() {
          return Optional.of(metricPrefix);
        }
      };

  private void registerMetrics(final MetricCategoryRegistry registry) {
    registry.addMetricCategory(metricCategory);
  }

  //
  // Events
  //

  private long listenerIdentifier;

  private void startEvents(final BesuEvents events) {
    listenerIdentifier = events.addBlockPropagatedListener(this::onBlockPropagated);
  }

  private void stopEvents(final BesuEvents events) {
    events.removeBlockPropagatedListener(listenerIdentifier);
  }

  //
  // Metrics
  //

  private Counter baseGasCounter;
  private Counter createGasCounter;
  private Counter dataGasCounter;
  private Counter executionGasCounter;

  private long currentBlockBaseGas;
  private long currentBlockCreateGas;
  private long currentBlockDataGas;
  private long currentBlockExecutionGas;

  private void startMetrics(final MetricsSystem metrics) {
    final LabelledMetric<Counter> counters =
        metrics.createLabelledCounter(
            metricCategory,
            "total",
            "Total amount of gas executed by all propagated blocks.",
            "source");
    baseGasCounter = counters.labels("base");
    createGasCounter = counters.labels("create");
    executionGasCounter = counters.labels("execution");
    dataGasCounter = counters.labels("data");

    metrics.createLongGauge(
        metricCategory,
        "last_block_base",
        "Gas spent because of base transaction fees for the most recently propagated block",
        () -> currentBlockBaseGas);
    metrics.createLongGauge(
        metricCategory,
        "last_block_create",
        "Gas spent because of contract creation fees for the most recently propagated block",
        () -> currentBlockCreateGas);
    metrics.createLongGauge(
        metricCategory,
        "last_block_data",
        "Gas Spent because of init code and data fees for the most recently propagated block",
        () -> currentBlockDataGas);
    metrics.createLongGauge(
        metricCategory,
        "last_block_execution",
        "Gas Spent because of EVM execution fees for the most recently propagated block",
        () -> currentBlockExecutionGas);
  }

  //
  // Gas Calculation
  //

  private static final long TX_DATA_ZERO_COST = 4L;
  private static final long TX_DATA_NON_ZERO_COST = 16L;
  private static final long TX_BASE_COST = 21_000L;
  private static final long TX_CREATE_COST = 32_000L;

  private void onBlockPropagated(final PropagatedBlockContext propagatedBlockContext) {
    long baseCost = 0;
    long createCost = 0;
    long dataCost = 0;
    for (final Transaction tx : propagatedBlockContext.getBlockBody().getTransactions()) {
      baseCost += TX_BASE_COST;
      createCost += tx.getInit().isPresent() ? TX_CREATE_COST : 0;
      for (final byte b : tx.getPayload().toArray()) {
        dataCost += (b == 0) ? TX_DATA_ZERO_COST : TX_DATA_NON_ZERO_COST;
      }
    }

    baseGasCounter.inc(baseCost);
    currentBlockBaseGas = baseCost;

    createGasCounter.inc(createCost);
    currentBlockCreateGas = createCost;

    dataGasCounter.inc(dataCost);
    currentBlockDataGas = dataCost;

    final long evmCost =
        propagatedBlockContext.getBlockHeader().getGasUsed() - baseCost - createCost - dataCost;
    executionGasCounter.inc(evmCost);
    currentBlockExecutionGas = evmCost;
  }
}
