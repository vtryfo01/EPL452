#!/usr/bin/env bash
# Prepare one node for HDFS and Spark: install Java 11 (HDFS) and 17 (Spark),
# and on the public interface block every port but SSH.
#
#   prepare_node.sh [master-host]    (default node0)
#
# Run as the experiment user; used by deploy.sh and startup.sh.
set -euo pipefail

MASTER=${1:-node0}
ARCH=$(dpkg --print-architecture)

install_java() {
  sudo -n apt-get -o DPkg::Lock::Timeout=300 update -q >/dev/null &&
    sudo -n env DEBIAN_FRONTEND=noninteractive apt-get -o DPkg::Lock::Timeout=300 install -y -q \
      openjdk-11-jre-headless openjdk-17-jre-headless >/dev/null
}

# Right after boot apt may still be busy with its daily update: retry a few times.
for _ in 1 2 3 4 5; do
  install_java && break
  sleep 60
done
for v in 11 17; do
  [[ -x /usr/lib/jvm/java-$v-openjdk-$ARCH/bin/java ]] || { echo "prepare_node: could not install Java $v" >&2; exit 1; }
done

# Spark and HDFS have no authentication: on the public interface allow only
# SSH and CloudLab's event service.
master_ip=$(getent ahostsv4 "$MASTER" | awk 'NR == 1 {print $1}')
[[ -n $master_ip ]] || { echo "prepare_node: cannot resolve $MASTER" >&2; exit 1; }
pub=$(ip -4 route show default | awk '{print $5; exit}')
lan=$(ip -4 -o route get "$master_ip" | sed -nE 's/.* dev ([^ ]+).*/\1/p')
if [[ $pub != "$lan" ]]; then
  rule=(INPUT -i "$pub" -p tcp --syn -m multiport ! --dports 22,16505 -j DROP)
  sudo -n iptables -C "${rule[@]}" 2>/dev/null || sudo -n iptables -I "${rule[@]}"
fi
