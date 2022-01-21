package eu.socialsensor.benchmarks;

import com.google.common.base.Stopwatch;
import eu.socialsensor.dataset.DatasetFactory;
import eu.socialsensor.graphdatabases.GraphDatabase;
import eu.socialsensor.main.BenchmarkConfiguration;
import eu.socialsensor.main.BenchmarkType;
import eu.socialsensor.main.GraphDatabaseType;
import eu.socialsensor.utils.Utils;

import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * FindShortestPathBenchmark implementation
 * 
 * @author sotbeis, sotbeis@iti.gr
 * @author Alexander Patrikalakis
 */
public class KoutBenchmark extends PermutingBenchmarkBase implements RequiresGraphData
{

    private final Set<Object> generatedNodes;
    private final int depth;

    public KoutBenchmark(BenchmarkConfiguration config) {
        super(config, BenchmarkType.KOUT);
        generatedNodes = DatasetFactory.getInstance().getDataset(config.getDataset())
            .generateRandomNodesLong(config.getKoutRandomNodes()).stream().map(
                    p-> (Object) p.toString()).collect(Collectors.toSet());
        depth = config.getKoutDepth();
    }

    @Override
    public void benchmarkOne(GraphDatabaseType type, int scenarioNumber)
    {
        GraphDatabase<?,?,?,?> graphDatabase = Utils.createDatabaseInstance(bench, type);
        graphDatabase.open(false);
        Stopwatch watch = Stopwatch.createStarted();
        graphDatabase.kouts(generatedNodes, depth);
        graphDatabase.shutdown();
        times.get(type).add((double) watch.elapsed(TimeUnit.MILLISECONDS));
    }
}
