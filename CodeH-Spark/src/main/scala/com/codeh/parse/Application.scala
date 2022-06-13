package com.codeh.parse

import org.apache.log4j.{LogManager, Logger}
import scopt.OptionParser

/**
 * @className Application
 * @author jinhua.xu
 * @date 2022/1/12 14:20
 * @description 命令行参数解析
 * @version 1.0
 */
object Application {
  def main(args: Array[String]): Unit = {
    val logger: Logger = LogManager.getLogger(this.getClass)
    logger.info("start parse config file")

    val parser: OptionParser[Params] = new scopt.OptionParser[Params]("Test_Parse") {
      opt[String]('c', "conf").action((x, c) => {
        c.copy(conf = Option(x))
      }).text("Path to the job config file (YAML/JSON)")

      help("help").text("use command line arguments to specify the configuration file path or content")
    }


    parser.parse(args, Params()) match {
      case Some(params) => {
        logger.info(params.conf)
        params.conf match {
          case Some(conf) => {
            logger.info(conf)
          }
          case None => {
            logger.error("没有传入命令行参数~~")
          }
        }
//        println(params)
        params.conf
      }
      case None =>{
        logger.error("请输入命令行参数~")
      }
    }
  }

}

case class Params(conf: Option[String] = None)
