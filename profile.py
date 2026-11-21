"""Spark Connect cluster: node0 is the master, node1..N are the workers.

Every node runs scripts/cloudlab/startup.sh at boot, which installs and starts
HDFS and Spark, checks the cluster from node0 and, if asked, prepares the
datasets. Progress: ~/startup.status and ~/startup.log on each node.
"""

import geni.portal as portal
import geni.rspec.pg as pg

pc = portal.Context()
pc.defineParameter("nodes", "Number of nodes (master + workers)", portal.ParameterType.INTEGER, 4)
pc.defineParameter("hardware", "Hardware type", portal.ParameterType.NODETYPE, "m400")
pc.defineParameter("datasets", "Datasets to prepare after the setup", portal.ParameterType.STRING, "all",
                   [("none", "None"),
                    ("base", "classics-60 and sample-2000 (about 20 min)"),
                    ("all", "Also the weak-scaling inputs weak-1 to weak-N (about 1 h)")])
params = pc.bindParameters()
if params.nodes < 2:
    pc.reportError(portal.ParameterError("Need at least 2 nodes", ["nodes"]))
pc.verifyParameters()

request = pc.makeRequestRSpec()
lan = request.LAN("lan")

for i in range(params.nodes):
    node = request.RawPC("node%d" % i)
    node.hardware_type = params.hardware
    node.disk_image = "urn:publicid:IDN+emulab.net+image+emulab-ops//UBUNTU22-64-STD"
    iface = node.addInterface("if%d" % i)
    iface.addAddress(pg.IPv4Address("10.10.1.%d" % (i + 1), "255.255.255.0"))
    lan.addInterface(iface)
    node.addService(pg.Execute(shell="bash", command="bash /local/repository/scripts/cloudlab/startup.sh %d %s"
                                                     % (params.nodes - 1, params.datasets)))

pc.printRequestRSpec(request)
