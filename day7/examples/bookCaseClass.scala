case class Book(name: String, price: Double)

object Book{
    def apply(name: String, price: Double): Book = {
        if(price < 0){
            throw new IllegalArgumentException(s"Price cannot be negative. Supplied arguments: name = $name ; price = $price")
        }
        println(s"Creating book titled: $name, with price: $price")
        new Book(name, price) // new keyword is required here to terminate infinity loop. However case class instantiation doesn't require `new` in general.
    }

    def unapply(book: Book): Option[(String, Double)] = {
        println(s"Decomposing book results - title: ${book.name} ; price: ${book.price}")
        Some((book.name, book.price))
    }
}

@main def bookCaseClass(): Unit = {
    try{
        val book1 = Book("The Maze Runner", 128.90)
        val book2 = Book("Harry Potter", -90.54)
    }
    catch{
        case e: Exception => println(e.getMessage)
    }

    val book = Book("John Carter", 500)
    book match {
        case Book(name, price) => println("Book is decomposed")
        case _ => println("Not a book object, can't be decomposed")
    }

}