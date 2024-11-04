import scala.io.StdIn.readLine
import scala.collection.mutable.ListBuffer
import scala.language.implicitConversions 

/* STATUS CODES -
    001: Parent Department does not exist in hierarchy
    002: Current Department does not exist under Parent Department
    003: Parent Department, Current Department mapping exists
*/


case class EmployeeDetails(sno: Int, name: String, city: String)
case class Node(
    department: String, 
    children: ListBuffer[Node] = ListBuffer.empty[Node], 
    employees: ListBuffer[EmployeeDetails] = ListBuffer.empty[EmployeeDetails]
)
case class ParentCurrentMappingExists(parentNode: Option[Node], status: Boolean, code: String)


implicit def tupleToEmployeeDetails(tup: (Int, String, String)): EmployeeDetails = EmployeeDetails(tup._1, tup._2, tup._3)


class OrganizationHierarchy {
    // Define the root node
    val root: Node = Node("Organization")

    // Check if the node exists based on the departmentName (BFS traversal)
    def checkNodeUsingDepartment(department: String): Option[Node] = {
        def bfs(queue: List[Node]): Option[Node] = queue match {
            case Nil => None
            case head :: rem => {
                if(head.department == department) Some(head)
                else bfs(rem ++ head.children)
            }
        }
        bfs(List(root))
    }


    // Check if mapping exists between the parentDepartment and currentDepartment
    def isMappingExists(parentDepartment: String, currentDepartment: String): ParentCurrentMappingExists = {
        val parentNode: Option[Node] = checkNodeUsingDepartment(parentDepartment)
        parentNode match {
            case Some(node) => {
                if(node.children.exists(x => x.department == currentDepartment)) ParentCurrentMappingExists(parentNode, true, "003")
                else ParentCurrentMappingExists(parentNode, false, "002")
            }
            case None => ParentCurrentMappingExists(None, false, "001")
        }
    }


    // Create employees under currentDepartment
    def getEmployees(currDept: String): List[EmployeeDetails] = {
        var terminateLoop: Boolean = false
        var employeeDetails: ListBuffer[EmployeeDetails] = ListBuffer.empty[EmployeeDetails]
        
        while(!terminateLoop) {
            val input = readLine(s"Adding employees under `$currDept`: Input 'Y' to continue the loop, 'N' to to enter the organizationHierarchy CLI: ")
            input match {
                case "Y" => {
                    val sno = readLine("Enter the sno of the employee: ")
                    val name = readLine("Enter the name of the employee: ")
                    val city = readLine("Enter the city of the employee: ")

                    employeeDetails.addOne((sno.toInt, name, city))
                }
                case "N" => { terminateLoop = true }
                case _ => { println(s"Adding employees under `$currDept`: Invalid command. Input 'exit' to enter the organizationHierarchy CLI") }
            }
            
        }
        employeeDetails.toList
    }


    // Create node in the organizationHierarchy
    def createNode() = {
        val parentDepartment = readLine("Enter the parent department name: ")
        val currentDepartment = readLine("Enter the current department name: ")
        val parentDeptCurrentDeptMapping: ParentCurrentMappingExists = isMappingExists(parentDepartment, currentDepartment)
        
        parentDeptCurrentDeptMapping match {
            case mp if mp.status == false && mp.code == "001" => {
                println(s"Department `$parentDepartment` does not exist")
            }
            case mp if mp.status == false && mp.code == "002" => {
                println(s"Department `$currentDepartment` does not exist under `$parentDepartment`.")
                val input = readLine(s"Input 'N' to not create the hierarchy {`$currentDepartment` under `$parentDepartment`}: ")
                if(input.toLowerCase != "n") {
                    val employees: ListBuffer[EmployeeDetails] = ListBuffer.empty.addAll(getEmployees(currentDepartment))
                    val childrenNode = Node(department = currentDepartment, employees = employees)

                    mp.parentNode.get.children.addOne(childrenNode)
                    println(s"Created `$currentDepartment` under `$parentDepartment`")
                }
            }
            case mp if mp.status == true => {
                val employees = getEmployees(currentDepartment)
                val currNode = mp.parentNode.get.children.find(x => x.department == currentDepartment).get

                currNode.employees.addAll(employees)
            }
            case _ => {}
        }
    }

    // Print the organization hierarchy
    def printOrgHierarchy(): Unit = {
        def indentation(depth: Int, content: Any): Unit = {
            (0 until depth).foreach(_ => print(" |"))
            print("-- ")
            println(content)
        }

        def printOrgHierarchyAcc(node: Node, depth: Int): Unit = {
            indentation(depth, node.department)
            node.employees.foreach(emp => indentation(depth + 1, emp))
            node.children.foreach(child => printOrgHierarchyAcc(child, depth + 1))
        }

        printOrgHierarchyAcc(root, 0)
    }
}


// Main function
object Main extends App {
    println("""Want to create organization hierarchy structure? 
    |Use commands `create` to add a record, `print` to get the hierarchy, `exit` to Exit
    """.stripMargin)

    val orgHierarchy = new OrganizationHierarchy()
    var terminateLoop: Boolean = false
    def hierarchyController() = {
        while(!terminateLoop) {
            val input = readLine("Enter the command: ")
            input match {
                case "create" => orgHierarchy.createNode()
                case "print" => orgHierarchy.printOrgHierarchy()
                case "exit" => {
                    println("Exiting....")
                    terminateLoop = true
                }
                case _ => println("Invalid command. Input 'exit' to terminate the loop")
            }
        }
    }

    hierarchyController()
}