package com.flink.pipeline.url

import java.io.InputStream
import java.net.{URL, URLConnection, URLStreamHandler, URLStreamHandlerFactory}

trait WithClasspathURL {
  URL.setURLStreamHandlerFactory(new ClasspathURLStreamHandlerFactory())
}

class ClasspathURLConnection(url: URL) extends URLConnection(url) {
  override def connect(): Unit = {}

  override def getInputStream: InputStream = {
    val path = url.getPath
    path.startsWith("/") match {
      case true => this.getClass.getResourceAsStream(path)
      case false => this.getClass.getClassLoader.getResourceAsStream(path)
    }
  }
}

class ClasspathURLStreamHandler extends URLStreamHandler {
  override def openConnection(url: URL): URLConnection = {
    new ClasspathURLConnection(url)
  }
}

class ClasspathURLStreamHandlerFactory extends URLStreamHandlerFactory {
  override def createURLStreamHandler(protocol: String): URLStreamHandler = {
    if ("classpath".equals(protocol)) {
      return new ClasspathURLStreamHandler
    }

    null
  }
}