package com.codeh.optimize.bean

/**
 * @className SaleCourse
 * @author jinhua.xu
 * @date 2021/12/9 17:34
 * @description TODO
 * @version 1.0
 */
case class SaleCourse(courseid: Long,
                      coursename: String,
                      status: String,
                      pointlistid: Long,
                      majorid: Long,
                      chapterid: Long,
                      chaptername: String,
                      edusubjectid: Long,
                      edusubjectname: String,
                      teacherid: Long,
                      teachername: String,
                      coursemanager: String,
                      money: String,
                      dt: String,
                      dn: String,
                      rand_courseid: String)
