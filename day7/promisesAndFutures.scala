import scala.concurrent.{Future, Promise, Await}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Success, Failure, Random}
import java.util.concurrent.atomic.AtomicBoolean

object Constants {
    val TARGET_NUMBER: Int = 2
    val NUM_THREADS: Int = 3
    val UPPER_LIMIT: Int = 5
}


def generateRandomNumber: Int = {
    val num = 1 + Random.nextInt( Constants.UPPER_LIMIT )  

    num
}

def randomNumberThreadExecutor(): Future[String] = {
    val promise = Promise[String]()

    // AtomicBoolean ensures thread-safe access to the track the promise completion state
    val isCompleted = new AtomicBoolean(false)

    def createThread(threadNum: Int) = new Thread(new Runnable{
        def run(): Unit = {
            var number = -1
        
            while (!isCompleted.get() && number != Constants.TARGET_NUMBER) {
                number = generateRandomNumber
                
                // If target number is generated, complete the promise in a synchronized block
                if (number == Constants.TARGET_NUMBER) {
                    /* compareAndSet method of AtomicBoolean performs an atomic check-and-set operation, which means it's thread-safe by design.
                     It ensures that the value is updated only if the current value matches the expected value. */
                    
                    // Set the flag to true and complete the promise
                    if (isCompleted.compareAndSet(false, true)) {
                        promise.success(s"Thread: $threadNum has generated ${Constants.TARGET_NUMBER}")
                    }
                }
            }
            
        }
    })

    val threads = (1 to Constants.NUM_THREADS).map(num => createThread(num))
    threads.foreach(thread => thread.start())

    promise.future

}

@main def processTask(): Unit = {
    val futureResult: Future[String] = randomNumberThreadExecutor()

    futureResult.onComplete{
        case Success(value) => println(value)
        case Failure(exception) => println(s"Exception: $exception")
    }

    Await.result(futureResult, Duration.Inf) // Wait for the futureResult to complete

}