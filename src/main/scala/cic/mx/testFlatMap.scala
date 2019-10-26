package cic.mx

object testFlatMap {
  def g(v:Int) = List(v-1, v, v+1)

  def main(args: Array[String]): Unit ={
    val l = List(1,2,3,4,5)

    print(l.flatMap(x => g(x)))
  }
}
