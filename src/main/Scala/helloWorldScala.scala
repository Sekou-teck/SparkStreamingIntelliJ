import scala._
import scala.collection.mutable._

object helloWorldScala {

  /* Premier programme Scala : il y a 4 type de base en Scala : byte = objets binaires, int = entiers naturels,
string = chaines de caractères, boolean = valeurs binaires (vrai ou faux) et unit = spécial qui donne aucune valeur
2 types de variables : val | var
 => immutable : pas modifiable (val)
 => mutable : modifiable (var) */
  val ma_var_imm: String = "Sekouba" // variablen immutable à portée publique
  private val une_var_imm: String = "Formation Big Data" // variable immutable à portée privée

  class person(var nom : String, var prenom : String, var age : Int)

  def main(args: Array[String]): Unit = {
    println("Hello World : mon premier programme en Scala")

    var test_mu: Int = 15 // var mutable
    test_mu = test_mu + 10
    println(test_mu)

    println("Votre texte contient : " + comptage_caracteres(texte = "Qu'avez-vous mangé ce matin ? ") + " caracteres")

    getResultat(parametre = 10)
    textWhile(valeur_cond = 10)
    textFor()
    collectionScala()
    collectionTuple()

    // function = renvoie des résultats et procédure = ne renvoie pas résultat. On les reconnait via Unit. Résultat = action

    // Déclaration : def nomFonction(param1, Param2 : typeParam, ...) : typeResult = {
    // action_A_Executer [ return resultFunction] }
    // ou def function(x : Int) : int = { }

    // maPremiereFonction :

    // syntaxe 1
    def comptage_caracteres(texte: String): Int = {
      if (texte.isEmpty) {
        0
      } else {
        texte.trim.length()
      }
      texte.trim.length()
    }

    // syntaxe 2
    def comptage_caractere2(texte: String): Int = {
      return texte.trim.length()
    }

    // syntaxe 3
    def comptage_caracteres3(texte: String): Int = texte.trim.length()

    // maPremièreProcedure | méthode

    def getResultat(parametre: Any): Unit = {
      if (parametre == 10) {
        println("Votre résultat est un entier naturel")
      } else {
        println("Votre résultat n'est pas un entier")
      }
    }

    // structuresConditionnelles : while et for
    // while :
    def textWhile(valeur_cond: Int): Unit = {
      var i: Int = 0
      while (i < valeur_cond) {
        println("itération while n° " + i)
        i = i + 1
      }
    }

    // for : contrôle le nombre d'itération
    def textFor(): Unit = {
      var i: Int = 1
      for (i <- 3 to 15 by 4) {
        println("itération For n° " + i)

      }
    }

    def collectionScala(): Unit = {
      val maliste: List[Int] = List(1, 2, 5, 7, 5, 8, 2, 10, 45, 15)

      val liste_s: List[String] = List("Sekouba", "Aïssata", "AÏcaley", "Ibrahim", "Jean", "Chris", "Julien", "Maurice", "trec")

      val plage_v: List[Int] = List.range(1, 15, 5)

      println(maliste(0))
      println(liste_s)
      println(plage_v)


      // Affichage de l'ensemble des eléments d'un liste
      /*
  for(i <- liste_s){
    println(i)
  }
*/
      // Manipulation des collections à l'aide des fonctions anonymes

      val resultats: List[String] = liste_s.filter(e => e.endsWith("n"))
      for (r <- resultats) {
        println(r)
      }
      // autre façon d'utiliser les fonctions anonymes

      var res: Int = liste_s.count(i => i.endsWith("a"))
      println("nombre d'éléments : " + res)

      val maliste2: List[Int] = maliste.map(e => e * 2) // on peut remplacer e => e * 2 par _ * 2
      for (r <- maliste2) {
        println(r)
      }

      val nouvelle_liste: List[Int] = plage_v.filter(p => p > 5) // des valeurs sup à 5

      val new_list: List[String] = liste_s.map(s => s.capitalize) // transformer en majuscule

      // collections ont une itérateur interne (foreach) pour manipuler chaque élément

      new_list.foreach(e => println("Nouvelle de : " + e)) // voir les éléments de nouvelle liste

      nouvelle_liste.foreach(e => println("Nouvelle de : " + e)) // Ces fonctions anonymes sont très importantes dans l'utilisation des listes
      // foreach, opérations ensemblistes (map, filter, ...), capitalize, fn anonymes, création de nouvelle liste, etc sont à maîtriser.
      plage_v.foreach(println(_))

    }

    // tuple = four-tout, on peut d'éléments autant qu'on veut
    // tuples sont des collections immutables de données hétérogènes (pas de notions d'ordre ni de typage)
    // il est four-tout.
    def collectionTuple(): Unit = {
      val tuple_test = (86, "Sekouba", 95, "Aïssata", 2017, "AÏcaley", 19, "Ibrahim")
      println(tuple_test._2)

      // autre tuple avec la classe person créé

      val nouvelle_personne : person = new person(nom = "FOFANA", prenom = "Ibrahim", age = 1)
      val tuple_2 = ("test", nouvelle_personne, 34)
      tuple_2.toString().toList  // Il est bon savoir les tuples, mais en Big Data, les listes sont les + utilisées

    }

    // table de hashage, clés-valeurs

    val states = Map(
      "AK" -> "Alaska",
      "GN" -> "Guinée",
      "IL" -> "Illinois",
      "KY" -> "Kentucky",
      "NY" -> "New York"
    )
    states.foreach(println(_))

    val personne = Map(
      "nom" -> "FOFANA",
      "prenom" -> "Aïcaley",
      "age" -> 3
    )
    personne.foreach(println(_))


    // Les tableaux ou Array

    //  Maquette : val arrayName : Array[Type] = Array(elt1, elt2, etc.)
    // exemple :

    val monTableau : Array[String] = Array("sekouba", "sfo", "sekou")
    monTableau.foreach(e => println(e))





  }


}
