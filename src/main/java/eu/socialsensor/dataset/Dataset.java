package eu.socialsensor.dataset;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.commons.collections.iterators.FilterIterator;
import org.apache.commons.math3.util.MathArrays;

import eu.socialsensor.utils.Utils;

/**
 * 
 * @author Alexander Patrikalakis
 *
 */
public class Dataset implements Iterable<List<String>>
{
    private final Iterator<List<String>> data;
    private final Set<Integer> nodes = new HashSet<>();
    private final Random random = new Random();

    public Dataset(File datasetFile)
    {
        data = Utils.getTabulatedLineIterator(datasetFile, 4 /* numberOfLinesToSkip */);
    }

    public Set<Integer> generateRandomNodes(int numRandomNodes)
    {
        List<Integer> nodeList = new ArrayList<Integer>(nodes);
        Set<Integer> generatedNodes = new HashSet<Integer>();
        for (int i = 0; i < numRandomNodes * 2; i++) {
            generatedNodes.add(nodeList.get(random.nextInt(nodeList.size())));
            if (generatedNodes.size() >= numRandomNodes) {
                break;
            }
        }
        return generatedNodes;
    }

    @Override
    public Iterator<List<String>> iterator()
    {
        return new FilterIterator(data, line ->{
            for (String nodeId : (List<String> )line) {
                nodes.add(Integer.parseInt(nodeId.trim()));
            }
            return true;
        });
    }
}
