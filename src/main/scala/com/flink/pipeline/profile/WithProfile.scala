package com.flink.pipeline.profile

import com.flink.pipeline.constant.Constants
import com.flink.pipeline.profile.impl.YamlProfile

import java.net.URL
import scala.util.{Failure, Success, Try}

trait WithProfile {

  lazy val profile: Profile = getProfile

  def getProfile: Profile = new YamlProfile(getProfileUrl)

  def getProfileUrl: URL = Try(new URL(System.getenv(Constants.ENV_PROFILE_URL))) match {
    case Success(url) => {
      url
    }
    case Failure(_) => {
      new URL(Constants.DEFAULT_PROFILE_YAML_URL)
    }
  }
}
