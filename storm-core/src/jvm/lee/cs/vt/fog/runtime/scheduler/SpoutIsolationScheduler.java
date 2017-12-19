package lee.cs.vt.fog.runtime.scheduler;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.storm.scheduler.Cluster;
import org.apache.storm.scheduler.ExecutorDetails;
import org.apache.storm.scheduler.IScheduler;
import org.apache.storm.scheduler.Topologies;
import org.apache.storm.scheduler.TopologyDetails;
import org.apache.storm.scheduler.WorkerSlot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SpoutIsolationScheduler implements IScheduler{

    private static final Logger LOG = LoggerFactory.getLogger(SpoutIsolationScheduler.class);

    private Map conf;

    @Override
    public void prepare(Map conf) {
        this.conf = conf;
    }

    @Override
    public void schedule(Topologies topologies, Cluster cluster) {
        LOG.info("SpoutIsolationScheduler begins...");

        List<TopologyDetails> tds = cluster.needsSchedulingTopologies(topologies);

        for (TopologyDetails td : tds) {
            List<WorkerSlot> slots = cluster.getAvailableSlots();
            if (slots.size() < 2) {
                LOG.info("slots.size() < 2");
                return;
            }

            int worker_num = td.getNumWorkers();
            if (worker_num != 2) {
                LOG.info("worker_num != 2 and forcelly make it 2");
                // return;
            }

            String t_id = td.getId();

            Map<ExecutorDetails, String> ed_map = td.getExecutorToComponent();

            Collection<ExecutorDetails> spout = new HashSet<ExecutorDetails>();
            Collection<ExecutorDetails> others = new HashSet<ExecutorDetails>();
            for (Entry<ExecutorDetails, String> entry : ed_map.entrySet()) {
                ExecutorDetails ed = entry.getKey();
                String c_id = entry.getValue();

                if (c_id.equals("spout"))
                    spout.add(ed);
                else
                    others.add(ed);
            }

            if (spout.isEmpty()) {
                LOG.info("spout collection isEmpty");
                return;
            }

            if (others.isEmpty()) {
                LOG.info("others collection isEmpty");
                return;
            }

            LOG.info("Assign spout collection");
            cluster.assign(slots.get(0), t_id, spout);

            LOG.info("Assign others collection");
            cluster.assign(slots.get(1), t_id, others);

        }

        LOG.info("SpoutIsolationScheduler ends.");
    }

}
