package org.crossref.räknare

import scala.collection.immutable.HashMap
import java.util.regex.Pattern

// TODO doesn't yet handle "*", but that's an obscure case anyway.
// TODO maybe use java.net.InetAddress ?
object ETld {
  case class Node(children: Map[String, Node])

  val numbers = Pattern.compile("^[0-9]*$")

  val lines = scala.io.Source.fromInputStream(Main.getClass.getClassLoader().getResourceAsStream("etld.txt"))
      .getLines()
      // Ignore the ! lines, there are only a small handful.
      .filterNot((x) => x.startsWith("//") || x.startsWith("!") ||  x.isEmpty() )
      .map(x => x.split("\\.").toList.reverse)
    
    // Insert path into trie structure.
    def updateIn(node: Node, parts: List[String]) : Node = {
      parts match {
        case x :: xs => node.children.get(x) match {
                                                  case Some(subnode) => new Node(node.children.updated(x, updateIn(subnode, xs)))
                                                  case None => new Node(node.children.updated(x, updateIn(new Node(new HashMap()), xs)))
                                                }
        case Nil => node
      }
    }
    
    // Trie, starting at the end of the domain.
    val structure = lines.foldLeft(new Node(new HashMap()))((a: Node, b: List[String]) => updateIn(a, b))

    // Split a domain into triple of (subdomain, domain name, eTld).
    def split(domain : String) = {
      // Recurse through tree, return the path through the tree (e.g. ["uk", "co"]).
      def r(domainParts : List[String], structure : Node, constructed : List[String]) : List[String] = {
        domainParts match {
          case x :: xs => structure.children.get(x) match {
            case None => constructed
            case Some(child) => r(xs, child, x :: constructed)
          }
          case Nil => constructed
        }
      }

      val domainParts = domain.split("\\.").toList.reverse

      // If it's an IP address then we're not really interested.
      if (numbers.matcher(domainParts.head).matches()) {
        DomainTriple("", "ip-address", "")
      } else {
        val eTldParts = r(domainParts, structure, Nil)
        val eTld = eTldParts.mkString(".")

        val rest = domain.substring(0, domain.length - eTld.length - 1)
        val restParts = rest.split("\\.")

        val domainName = restParts.last
        val subdomain = restParts.dropRight(1).mkString(".")

        DomainTriple(subdomain, domainName, eTld)
      }
    } 
}