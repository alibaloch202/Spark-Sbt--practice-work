package example

import org.scalatest._
import org.scalatest.Matchers._
import org.scalactic.TypeCheckedTripleEquals._

class myFirstTest extends FlatSpec with Matchers {
    "Result dataframe" should "have distinct 10 rows in it" in {
        
        var (distinctRowsCount , dataMatch) = Hello.testNumberofDistinctRows()
        
        distinctRowsCount shouldEqual 10
        // Output Directory Data Match
        dataMatch shouldBe true
        
    }
    "Output directory" should "have output files" in {
        Hello.Output_Directory_Check shouldBe true
    }

    
}