import org.apache.spark.network.pmof.RdmaServer

object ServerTest {
  def main(args: Array[String]): Unit = {
    val server: RdmaServer = new RdmaServer("172.168.2.106", 12345)
    server.init()
    server.start()
    server.waitToStop()
  }
}
