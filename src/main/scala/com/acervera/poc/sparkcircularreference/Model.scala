package com.acervera.poc.sparkcircularreference

case class Branch(id: Int, branches: List[Branch] = List.empty)
case class Root(id: Int, branches: List[Branch] = List.empty)
case class Tree(id: Int, root: Root)