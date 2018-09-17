import org.apache.spark.network.pmof.RDMAClient
import org.apache.spark.network.pmof.RDMAClientFactory

object ClientTest {
  val connectionNum: Int = 10
  def main(args: Array[String]): Unit = {
    var client: RDMAClient = null
    val factory: RDMAClientFactory = new RDMAClientFactory
    for (i <- 0 to connectionNum) {
      val newClient = factory.createClient("172.168.2.106", 12345)
      if (i == 0)
        client = newClient
      else
        assert(newClient == client)
    }
    factory.waitToStop()
  }
}
