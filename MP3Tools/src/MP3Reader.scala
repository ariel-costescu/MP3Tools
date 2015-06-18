import java.io.File
import java.io.PrintWriter
import java.util.logging.FileHandler
import java.util.logging.SimpleFormatter
import scala.collection.mutable.ListBuffer
import scala.xml._
import com.mpatric.mp3agic.Mp3File
import java.util.Date
import scala.collection.parallel.ForkJoinTaskSupport
import scala.concurrent.forkjoin.ForkJoinPool
import scala.io.Source
import scala.xml.XML
import scala.xml.XML
import java.io.PrintStream
import java.io.ByteArrayOutputStream

object MP3Reader {
  
  def readFilesRecursively(dir:File, list:ListBuffer[File]) {
    val dirList = dir.listFiles()
    list ++= dirList.filter(f => f.isFile())
    dirList.filter(f => f.isDirectory()).foreach { d => readFilesRecursively(d, list) }
  }
  
  def processMP3Magic(f:File):(Any, Long) = {
    val start = System.nanoTime()
    var ex:Exception = null
    var af:Mp3File = null
    var ret:Any = null
    try {
      af = new Mp3File(f)
    }
    catch {
      case e:Exception => {
        ex = e
      }
    }
    finally {
      if (ex != null) {
        ret = (ex, f)
      }
      else {
        ret = af
      }
    }
    val end = System.nanoTime()
    (ret, end - start)
  }
  
  def processTagLib(f:File):(Any, Long) = {
    val start = System.nanoTime()
    var ex:Exception = null
    var af:Array[String] = null
    var tlr:TagLibReader = new TagLibReader
    var ret:Any = null
    try {
      af = tlr.getTags(f.getAbsolutePath)
    }
    catch {
      case e:Exception => {
        ex = e
      }
    }
    finally {
      if (ex != null) {
        ret = (ex, f)
      }
      else {
        ret = (af,f)
      }
    }
    val end = System.nanoTime()
    (ret, end - start)
  }
  
  def MP3MagicFileToXML(t:(Any, Long)):Elem = {
    val x = t._1 
    val af:Mp3File = x.asInstanceOf[Mp3File]
    var artist:String = null
    var albumArtist:String = null
    var album:String = null
    var title:String = null
    if (af.hasId3v1Tag()) {
      artist = if (af.getId3v1Tag.getArtist != null) af.getId3v1Tag.getArtist else ""
      album = if (af.getId3v1Tag.getAlbum != null) af.getId3v1Tag.getAlbum else ""
      title = if (af.getId3v1Tag.getTitle != null) af.getId3v1Tag.getTitle else ""
      <file processTimeNS={t._2.toString()} hasId3v1Tag="true" path={af.getFilename}>
      	<artist>{new PCData(artist)}</artist>
        <album>{new PCData(album)}</album>
        <title>{new PCData(title)}</title>
      </file>
    }
    else if (af.hasId3v2Tag()) {
      artist = if (af.getId3v2Tag.getArtist != null) af.getId3v2Tag.getArtist else ""
      albumArtist = if (af.getId3v2Tag.getAlbumArtist != null) af.getId3v2Tag.getAlbumArtist else ""
      album = if (af.getId3v2Tag.getAlbum != null) af.getId3v2Tag.getAlbum else ""
      title = if (af.getId3v2Tag.getTitle != null) af.getId3v2Tag.getTitle else ""
      <file processTimeNS={t._2.toString()} hasId3v2Tag="true" path={af.getFilename}>
      	<artist>{new PCData(artist)}</artist>
        {if (artist != albumArtist && !albumArtist.isEmpty()) <albumArtist>{new PCData(albumArtist)}</albumArtist>}
        <album>{new PCData(album)}</album>
        <title>{new PCData(title)}</title>
      </file>
    }
    else {
      <file processTimeNS={t._2.toString()} hasId3v1Tag="false" hasId3v2Tag="false" path={af.getFilename}/>
    }
  }
  
  def tagLibTagsToXML(t:(Any, Long)):Elem = {
    val x = t._1 
    val af:(Array[String], File) = x.asInstanceOf[(Array[String], File)]
    val tags = af._1
    val path = af._2.getAbsolutePath
    val checkChromaprint = ! tags.forall { x => x != null && !x.isEmpty }
    if (!checkChromaprint) {
    <file processTimeNS={t._2.toString()} path={path}>
        <artist>{new PCData(if (tags(0) != null) tags(0) else "")}</artist>
        <album>{new PCData(if (tags(1) != null) tags(1) else "")}</album>
        <title>{new PCData(if (tags(2) != null) tags(2) else "")}</title>
    </file>
    } else {
      try {
      val c = new ChromaPrint
      val fp = c.getFingerprint(path)(0)
      val d = c.getFingerprint(path)(1)
      val url = "http://api.acoustid.org/v2/lookup?format=xml&client=ULjKruIh&meta=recordings+releases&duration=" + d + "&fingerprint=" + fp;
      val u = Source.fromURL(url).mkString
      val xu = XML.loadString(u)
    <file processTimeNS={t._2.toString()} path={path}>
        <artist>{new PCData(if (tags(0) != null) tags(0) else "")}</artist>
        <album>{new PCData(if (tags(1) != null) tags(1) else "")}</album>
        <title>{new PCData(if (tags(2) != null) tags(2) else "")}</title>
				<acoustid>{xu}</acoustid>
    </file>
      }
      catch {
        case e:Exception => {
          val baos:ByteArrayOutputStream  = new ByteArrayOutputStream()
          val p:PrintStream = new PrintStream(baos,true,"utf-8")
          e.printStackTrace(p)
          <file processTimeNS={t._2.toString()} path={path}>
						<error>{new PCData(baos.toString)}</error>
					</file>
        }
      }
    }
  }
  
  def processMusicCollection[T](startPath:String, outPath:String, errPath:String, processFile:File=>(Any, Long), fileToXML:((Any, Long))=>Elem):Unit = {
    var lb:ListBuffer[File] = new ListBuffer
    readFilesRecursively(new File(startPath), lb)
    val mp3sPar = lb//.par
    //mp3sPar.tasksupport_=(new ForkJoinTaskSupport(new ForkJoinPool(Runtime.getRuntime.availableProcessors() * 4)))
    val mp3s = mp3sPar.filter { f => f.getName.endsWith(".mp3") }
    
    val out = new PrintWriter(outPath)
    val err = new PrintWriter(errPath)
    val start = System.nanoTime()
    val processed = mp3s.map(processFile) 
    val end = System.nanoTime()
    println((end - start).doubleValue()/1000000)
    val processResult = processed.reduce((x,y) => ("Total time ",x._2 + y._2))
    println(processResult._1.toString() + (processResult._2.doubleValue() / 1000000) )
    
    val tagged = processed.filter( x => x._1.isInstanceOf[T] ).map(fileToXML)
    val taggedXML = 
    <taggedXML>
      {tagged.seq}
    </taggedXML>
    val printer = new scala.xml.PrettyPrinter(120, 4)
    out.print(printer.format(taggedXML))
    out.flush
    out.close
    val error = processed.filter { x => !x._1.isInstanceOf[T] }.map{ t => {
      val x = t._1
      val e = x.asInstanceOf[Tuple2[Exception, File]]
      <exception path={e._2.getAbsolutePath} message={e._1.getMessage}/>
    }}
    if (error.length > 0) {
      val errorXML = <errorXML>{error}</errorXML>
      err.print(printer.format(errorXML))
    }
    err.flush
    err.close
  }
  
  def processMusicCollectionMP3Magic(startPath:String, outPath:String, errPath:String):Unit = {
    processMusicCollection[Mp3File](startPath, outPath, errPath, processMP3Magic, MP3MagicFileToXML)
  }
  
  def processMusicCollectionTagLib(startPath:String, outPath:String, errPath:String):Unit = {
    processMusicCollection[(Array[String], File)](startPath, outPath, errPath, processTagLib, tagLibTagsToXML)
  }
  
  def main(args: Array[String]): Unit = {
    processMusicCollectionTagLib("I:\\Music\\Radiohead", "I:\\out-rh.txt", "I:\\err-rh.txt")
    //processMusicCollectionTagLib("I:\\Music", "I:\\out-tl.txt", "I:\\err-tl.txt")
  }
}

