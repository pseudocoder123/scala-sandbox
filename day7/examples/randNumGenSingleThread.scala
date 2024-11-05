import scala.concurrent.{Future, Promise}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Success, Failure, Random}


def generateRandomNumber: Int = {
    Thread.sleep(1000)
    val num = Random.nextInt( 1001 )  // generates between 0 to 1000
    println(s"Number is $num")

    num
}

def randomNumberThreadExecutor(): Future[Int] = {
    val promise = Promise[Int]()

    val thread = new Thread(new Runnable {
        def run(): Unit = {
          var attempts = 0
          val maxAttempts = 10
          var number = -1

          while (number != 10 && attempts < maxAttempts) {
            number = generateRandomNumber
            attempts += 1
          }

          if (number == 10) {
            promise.success(1) // Complete the promise with a success value of 1
          } else {
            promise.failure(new Exception("Reached max attempts without generating a 10"))
          }
        }
    })
    thread.start()

    promise.future

}

@main def processTask(): Unit = {
    val futureResult: Future[Int] = randomNumberThreadExecutor()

    futureResult.onComplete{
        case Success(value) => println(value)
        case Failure(exception) => println(s"Exception: $exception")
    }

}