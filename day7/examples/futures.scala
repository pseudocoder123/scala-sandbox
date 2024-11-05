import scala.concurrent.{Future, ExecutionContext}
import scala.util.{Success, Failure}
import scala.concurrent.ExecutionContext.Implicits.global

def processTask(): Future[String] = Future {
    println("Started the process.....")
    Thread.sleep(2000)
    "Process completed!"
}

def processTask2(): Future[String] = Future {
    println("Started the process2.....")
    Thread.sleep(1000)
    "Process2 completed!"
}

def processTask3(): Future[String] = Future {
    println("Started the process3.....")
    Thread.sleep(500)
    "Process3 completed!"
}

@main def caller() = {
    processTask().onComplete{
        case Success(value) => println("Received :: " + value)
        case Failure(exception) => println(exception.getMessage())
    }


    println("Something else.....")
    println("Another message!")
    // Thread.sleep(3000)

    println("Something else 2.....")
    processTask2().onComplete{
        case Success(value) => println("Received :: " + value)
        case Failure(exception) => println(exception.getMessage())
    }
    
    println("Something else 3.....")
    processTask3().onComplete{
        case Success(value) => println("Received :: " + value)
        case Failure(exception) => println(exception.getMessage())
    }

    println("Something else 4.....")

    Thread.sleep(6)
}