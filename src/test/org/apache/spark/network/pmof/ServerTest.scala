import org.apache.spark.network.pmof.RDMAServer

object ServerTest {
  def main(args: Array[String]): Unit = {
    val server: RDMAServer = new RDMAServer("172.168.2.106", 12345)
    server.init()
    server.start()
    server.waitToStop()
  }
}
