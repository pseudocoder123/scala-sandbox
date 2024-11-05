case class JobRunner(str: String, timeInSeconds: Int)

object JobRunner{
    def apply(str: String, timeInSeconds: Int)(logicalBlock: String => String) = {
        val baseTime: Long = System.currentTimeMillis() / 1000
        var currTime: Long = baseTime

        while(currTime - baseTime < timeInSeconds){
            println("I'm waiting......")
            Thread.sleep(1000)
            currTime = System.currentTimeMillis() / 1000
        }
        println(logicalBlock(str))
    }
}

@main def processTask(): Unit = {
    val jobRunnerObj = JobRunner("Saketh", 10){ 
        println("I will be printed on call")
        name => {
            println("I will be printed after 10 sec")
            s"Hi, I'm $name"
        }
    }
}

// (string)=>{  string}

// {
//     Println("")
//     (string)=>(string)
// }