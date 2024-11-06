case class Transaction(name: String, amount: Double, finalPrice: Double)

object Transaction {
    def apply(name: String, amount: Double)(logic: Double => Double): Transaction = {
        val taxPrice = logic(amount)
        val finalPrice = amount + taxPrice

        println(s"Creating transaction object: Name = $name ; Amount = $amount ; Tax = $taxPrice ; finalPrice = $finalPrice")
        new Transaction(name, amount, finalPrice)
    }
}

// If price is above 1000, 10% tax
@main def process(): Unit = {
    def taxCalculator: Double => Double = (amt) => {
        if(amt > 1000) amt/10.0
        else 0.0
    }

    val transaction1 = Transaction("Item-A", 2000){amount =>{
        if(amount > 1000) amount/10.0
        else 0.0
    }}

    val transaction2 = Transaction("Item-A", 2000){taxCalculator}
}