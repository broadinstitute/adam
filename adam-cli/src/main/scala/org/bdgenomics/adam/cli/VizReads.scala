/**
 * Licensed to Big Data Genomics (BDG) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The BDG licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.bdgenomics.adam.cli

import org.bdgenomics.adam.rich.ReferenceMappingContext._
import org.bdgenomics.adam.rich.RichADAMRecord
import org.bdgenomics.formats.avro.ADAMRecord
import org.kohsuke.args4j.{ Option => Args4jOption, Argument }
import org.apache.spark.SparkContext
import org.apache.hadoop.mapreduce.Job
import java.util.logging.Level
import java.util.regex.Pattern
import org.apache.spark.rdd.RDD
import java.awt._
import java.awt.image.BufferedImage
import javax.imageio.ImageIO
import javax.swing._
import scala.math._
import java.io.File
import java.awt.event._
import org.bdgenomics.adam.models.{ OrderedTrackedLayout, TrackedLayout, ReferenceRegion }
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.util.ParquetLogger
import org.bdgenomics.adam.projections.Projection
import org.bdgenomics.adam.projections.ADAMRecordField._

object VizReads extends ADAMCommandCompanion {
  val commandName: String = "viz"
  val commandDescription: String = "Generates images from sections of the genome"

  def apply(cmdLine: Array[String]): ADAMCommand = {
    new VizReads(Args4j[VizReadsArgs](cmdLine))
  }

  private val regionPattern = Pattern.compile("([^:]+):(\\d+)-(\\d+)")

  def parseRegion(regionString: String): ReferenceRegion = {
    val matcher = regionPattern.matcher(regionString)
    if (!matcher.matches()) { throw new IllegalArgumentException("\"%s\" doesn't match regionPattern".format(regionString)) }
    val refName = matcher.group(1)
    val start = matcher.group(2).toLong
    val end = matcher.group(3).toLong

    ReferenceRegion(refName, start, end)
  }

  def draw(region: ReferenceRegion, layout: TrackedLayout[ADAMRecord], drawing: Drawable) {

    val base = 10
    val trackHeight = min(20, (drawing.height - base) / (layout.numTracks + 1))

    val baselineState = new DrawingState(Color.black, new BasicStroke(1.0f), Font.decode("Courier 12"))
    val readState = baselineState.withColor(Color.blue)

    // draws the "baseline" at the bottom of the view
    drawing.line(baselineState, 0, drawing.height - base / 2, drawing.width, drawing.height - base / 2)

    // draws a box for each read, in the appropriate track.
    layout.trackAssignments.foreach {
      case (rec: ADAMRecord, track: Int) =>

        val ry1 = drawing.height - base - trackHeight * (track + 1)

        val rxf = (rec.getStart - region.start).toDouble / region.width.toDouble
        val rx1: Int = round(rxf * drawing.width).toInt

        val rxwf = rec.referenceLength.toDouble / region.width.toDouble
        val rw: Int = max(round(rxwf * drawing.width) - 1, 1).toInt // at least make it one-pixel wide.

        drawing.box(readState, rx1, ry1, rw, trackHeight)
    }
  }

  val whiteState = DrawingState(Color.white, new BasicStroke(1.0f), Font.decode("Courier 12"))

}

class VizReadsArgs extends Args4jBase with SparkArgs with ParquetArgs {
  @Argument(required = true, metaVar = "INPUT", usage = "The ADAM Records file to view", index = 0)
  var inputPath: String = null

  @Argument(required = true, metaVar = "REGION", usage = "The region to view (in format \"[ref]:[start]-[end]\")", index = 1)
  var regionString: String = null

  @Args4jOption(required = false, name = "-static", usage = "The name of the PNG to output -- only outputs a static picture")
  var staticOutput: String = null
}

class VizReads(protected val args: VizReadsArgs) extends ADAMSparkCommand[VizReadsArgs] {
  val companion: ADAMCommandCompanion = VizReads

  def run(sc: SparkContext, job: Job): Unit = {

    ParquetLogger.hadoopLoggerLevel(Level.SEVERE)

    val region = VizReads.parseRegion(args.regionString)

    val proj = Projection(contig, readName, start, cigar, primaryAlignment, firstOfPair, properPair)

    //val reads : RDD[ADAMRecord] = sc.adamRecordsRegionParquetLoad(args.inputPath, region, Set(ADAMRecordField.referenceName, ADAMRecordField.readName))
    val reads: RDD[ADAMRecord] = sc.adamLoad(args.inputPath, projection = Some(proj))

    if (args.staticOutput == null) {
      val panel = new ViewingPanel(new VizState(reads, region))
      new ViewingFrame(panel)

    } else {
      val (w, h) = (1000, 750)
      val image = new BufferedImage(w, h, BufferedImage.TYPE_INT_ARGB)
      val drawable = new ImageDrawable(image)
      drawable.box(VizReads.whiteState, 0, 0, w, h, filled = true)

      VizReads.draw(region, new OrderedTrackedLayout(reads.filterByOverlappingRegion(region).collect()), drawable)

      ImageIO.write(image, "png", new File(args.staticOutput))
    }
  }

}

trait StateUpdateListener {
  def updateStarting()
  def stateUpdated(view: ReferenceRegion)
}

class VizState(reads: RDD[ADAMRecord], var view: ReferenceRegion) {

  private var stateUpdateListeners: Seq[StateUpdateListener] = Seq()
  var viewable: Seq[ADAMRecord] = reads.filterByOverlappingRegion(view).collect()
  var layout = new OrderedTrackedLayout[ADAMRecord](viewable)

  def addUpdateListener(listener: StateUpdateListener) {
    stateUpdateListeners = stateUpdateListeners :+ listener
  }

  def setView(newView: ReferenceRegion) {
    stateUpdateListeners.foreach(_.updateStarting())
    new ViewableCalculation(newView).execute()
  }

  class ViewableCalculation(newView: ReferenceRegion) extends SwingWorker[OrderedTrackedLayout[ADAMRecord], Object] {
    protected def doInBackground(): OrderedTrackedLayout[ADAMRecord] = {
      new OrderedTrackedLayout(reads.filterByOverlappingRegion(newView).collect())
    }

    override protected def done() {
      layout = get()
      viewable = layout.reads.toSeq
      view = newView
      stateUpdateListeners.foreach(listener => listener.stateUpdated(newView))
    }
  }

  def draw(drawable: Drawable) {
    VizReads.draw(view, layout, drawable)
  }

  def moveLeft() {
    val w = view.width
    val newStart = max(view.start - w / 2, 0)
    val newEnd = newStart + w
    setView(ReferenceRegion(view.referenceName, newStart, newEnd))
  }

  def moveRight() {
    val w = view.width
    val newEnd = view.end + w / 2
    val newStart = newEnd - w
    setView(ReferenceRegion(view.referenceName, newStart, newEnd))
  }

  def zoomIn() {
    val w = view.width
    val nw = max(100, w / 2)
    val newStart = view.start + w / 4
    val newEnd = newStart + nw
    setView(ReferenceRegion(view.referenceName, newStart, newEnd))
  }

  def zoomOut() {
    val w = view.width
    val nw = w * 2
    val newStart = view.start - w / 2
    val newEnd = newStart + nw
    setView(ReferenceRegion(view.referenceName, newStart, newEnd))
  }
}

case class DrawingState(color: Color, stroke: Stroke, font: Font) {
  def withColor(newColor: Color) = DrawingState(newColor, stroke, font)
  def withStroke(newStroke: Stroke) = DrawingState(color, newStroke, font)
  def withFont(newFont: Font) = DrawingState(color, stroke, newFont)
}

class ViewingPanel(val vizState: VizState) extends JPanel with StateUpdateListener {

  vizState.addUpdateListener(this)

  var drawable: Drawable = null

  setPreferredSize(new Dimension(600, 400))

  def stateUpdated(view: ReferenceRegion) {
    drawable = null
    repaint()
  }

  def updateStarting() {

  }

  override def paintComponent(g: Graphics) {
    if (drawable == null) {
      drawable = new GraphicsDrawable(getWidth, getHeight, g.asInstanceOf[Graphics2D])
    }

    val color = g.getColor
    g.setColor(Color.white)
    g.fillRect(0, 0, drawable.width, drawable.height)
    g.setColor(color)

    vizState.draw(drawable)
  }

  def updateSize() {
    drawable = null
    repaint()
  }

  def createAction(name: String)(thunk: => Unit): Action = {
    new AbstractAction(name) {
      def actionPerformed(evt: ActionEvent) { thunk }
    }
  }

  def zoomIn() = createAction("++") {
    vizState.zoomIn()
  }

  def zoomOut() = createAction("--") {
    vizState.zoomOut()
  }

  def left() = createAction("<-") {
    vizState.moveLeft()
  }

  def right() = createAction("->") {
    vizState.moveRight()
  }
}

class ViewingFrame(val viewingPanel: ViewingPanel) extends JFrame("Genome Viewer") with StateUpdateListener {

  setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE)

  private val container = getContentPane
  container.setLayout(new BorderLayout())
  container.add(viewingPanel, BorderLayout.CENTER)

  private val controlPanel = new JPanel(new FlowLayout())
  container.add(controlPanel, BorderLayout.SOUTH)

  private val controls = Seq(new JButton(viewingPanel.left()),
    new JButton(viewingPanel.zoomOut()),
    new JButton(viewingPanel.zoomIn()),
    new JButton(viewingPanel.right()))

  controls.foreach(controlPanel.add(_))
  viewingPanel.vizState.addUpdateListener(this)

  addComponentListener(new ComponentAdapter() {
    override def componentResized(evt: ComponentEvent) {
      viewingPanel.updateSize()
    }
  })

  SwingUtilities.invokeLater(new Runnable() {
    def run() {
      setVisible(true)
      pack()
    }
  })

  def stateUpdated(region: ReferenceRegion) {
    controls.foreach(_.setEnabled(true))
  }

  def updateStarting() {
    controls.foreach(_.setEnabled(false))
  }
}

trait Drawable {

  def width: Int
  def height: Int

  def line(state: DrawingState, x1: Int, y1: Int, x2: Int, y2: Int)
  def box(state: DrawingState, x1: Int, y1: Int, w: Int, h: Int, filled: Boolean = false)
  def circle(state: DrawingState, xc: Int, yc: Int, radius: Int, filled: Boolean = false)
}

class ImageDrawable(im: BufferedImage) extends GraphicsDrawable(im.getWidth(null), im.getHeight(null), im.getGraphics.asInstanceOf[Graphics2D]) {
  val image = im
}

class GraphicsDrawable(val width: Int, val height: Int, graphics: Graphics2D) extends Drawable with Serializable {

  private var lastState: DrawingState = DrawingState(graphics.getColor, graphics.getStroke, graphics.getFont)

  private def updateState(newState: DrawingState) {
    if (lastState != newState) {
      lastState = newState
      graphics.setColor(newState.color)
      graphics.setStroke(newState.stroke)
      graphics.setFont(newState.font)
    }
  }

  def line(state: DrawingState, x1: Int, y1: Int, x2: Int, y2: Int) {
    updateState(state)
    graphics.drawLine(x1, y1, x2, y2)
  }

  def box(state: DrawingState, x1: Int, y1: Int, w: Int, h: Int, filled: Boolean = false) {
    updateState(state)
    if (filled) {
      graphics.fillRect(x1, y1, w, h)
    } else {
      graphics.drawRect(x1, y1, w, h)
    }
  }

  def circle(state: DrawingState, xc: Int, yc: Int, radius: Int, filled: Boolean) {
    updateState(state)
    if (filled) {
      graphics.fillOval(xc - radius, yc - radius, 2 * radius, 2 * radius)
    } else {
      graphics.drawOval(xc - radius, yc - radius, 2 * radius, 2 * radius)
    }
  }
}