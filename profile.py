"""Spark Connect cluster: node0 is the master, node1..N are the workers."""

import geni.portal as portal
import geni.rspec.pg as pg

pc = portal.Context()
pc.defineParameter("nodes", "Number of nodes (master + workers)", portal.ParameterType.INTEGER, 4)
pc.defineParameter("hardware", "Hardware type", portal.ParameterType.NODETYPE, "m400")
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

pc.printRequestRSpec(request)
