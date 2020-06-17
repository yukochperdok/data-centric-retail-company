package com.training.bigdata.omnichannel.stockATP.calculateStockATP.util

import com.training.bigdata.omnichannel.stockATP.common.util.{ArticleTypeAndCategory, Constants, DefaultConf}
import com.training.bigdata.omnichannel.stockATP.common.util.tag.TagTest.UnitTestTag
import org.scalatest.{FlatSpec, Matchers}
import com.training.bigdata.omnichannel.stockATP.common.util.LensAppConfig._

class OrdersUtilsTest extends FlatSpec with Matchers with DefaultConf {

  trait OrdersVariables {
    val ARTICLE_TYPE_AND_CATEGORY_TEST_1: List[ArticleTypeAndCategory] =
      List(
        ArticleTypeAndCategory("AAAA", "11"),
        ArticleTypeAndCategory("BBBB", "11"),
        ArticleTypeAndCategory("AAAA", "12"),
        ArticleTypeAndCategory("CCCC", "22")
      )
    val articleTypeAndCategoryOk1 = ArticleTypeAndCategory("AAAA", "11")
    val articleTypeAndCategoryOk2 = ArticleTypeAndCategory("BBBB", "11")
    val articleTypeAndCategoryOk3 = ArticleTypeAndCategory("AAAA", "12")
    val articleTypeAndCategoryOk4 = ArticleTypeAndCategory("CCCC", "22")

    val articleTypeAndCategoryWrong1 = ArticleTypeAndCategory("AAAA", "15")
    val articleTypeAndCategoryWrong2 = ArticleTypeAndCategory("DDDD", "11")
    val articleTypeAndCategoryWrong3 = ArticleTypeAndCategory("AAAAA", "11")
    val articleTypeAndCategoryWrong4 = ArticleTypeAndCategory("AAAA", "111")
    val articleTypeAndCategoryNull1 = ArticleTypeAndCategory(null, null)
    val articleTypeAndCategoryNull2 = ArticleTypeAndCategory("AAAA", null)
    val articleTypeAndCategoryNull3 = ArticleTypeAndCategory(null, "11")

    val articleTypeAndCategoryUNIQUE_SALE_ARTICLE_1 = Constants.PURCHASE_ARTICLE_TYPE_AND_CATEGORY_WITH_UNIQUE_SALE_ARTICLE(0)

    val articleTypeAndCategoryPREPACK_BOX_1 = Constants.BOX_AND_PREPACK_TYPE_AND_CATEGORY(0)
    val articleTypeAndCategoryPREPACK_BOX_2 = Constants.BOX_AND_PREPACK_TYPE_AND_CATEGORY(1)

    val INDICATOR_SALE_1 = "S"
    val INDICATOR_SALE_2 = ""
    val LIST_INDICATORS_SALES_ARTICLE: List[String] = INDICATOR_SALE_1 :: INDICATOR_SALE_2 :: Nil
  }

  "isOfTypeAndCategoryArticle " must
    " return true if there is the input article on the list of article's types and categories " +
      "with not null salesParentArticle" taggedAs (UnitTestTag) in new OrdersVariables{

    OrdersUtils.isOfTypeAndCategoryArticle(
      articleTypeAndCategoryOk1, ARTICLE_TYPE_AND_CATEGORY_TEST_1) shouldBe true
    OrdersUtils.isOfTypeAndCategoryArticle(
      articleTypeAndCategoryOk2, ARTICLE_TYPE_AND_CATEGORY_TEST_1) shouldBe true
    OrdersUtils.isOfTypeAndCategoryArticle(
      articleTypeAndCategoryOk3, ARTICLE_TYPE_AND_CATEGORY_TEST_1) shouldBe true
    OrdersUtils.isOfTypeAndCategoryArticle(
      articleTypeAndCategoryOk4, ARTICLE_TYPE_AND_CATEGORY_TEST_1) shouldBe true
    OrdersUtils.isOfTypeAndCategoryArticle(
      articleTypeAndCategoryWrong1, ARTICLE_TYPE_AND_CATEGORY_TEST_1) shouldBe false
    OrdersUtils.isOfTypeAndCategoryArticle(
      articleTypeAndCategoryWrong2, ARTICLE_TYPE_AND_CATEGORY_TEST_1) shouldBe false
    OrdersUtils.isOfTypeAndCategoryArticle(
      articleTypeAndCategoryWrong3, ARTICLE_TYPE_AND_CATEGORY_TEST_1) shouldBe false
    OrdersUtils.isOfTypeAndCategoryArticle(
      articleTypeAndCategoryWrong4, ARTICLE_TYPE_AND_CATEGORY_TEST_1) shouldBe false
    OrdersUtils.isOfTypeAndCategoryArticle(
      articleTypeAndCategoryNull1, ARTICLE_TYPE_AND_CATEGORY_TEST_1) shouldBe false
    OrdersUtils.isOfTypeAndCategoryArticle(
      articleTypeAndCategoryNull2, ARTICLE_TYPE_AND_CATEGORY_TEST_1) shouldBe false
    OrdersUtils.isOfTypeAndCategoryArticle(
      articleTypeAndCategoryNull3, ARTICLE_TYPE_AND_CATEGORY_TEST_1) shouldBe false

  }


  "isASaleArticle " must
    " return true when the input article is a sale article or null" taggedAs (UnitTestTag) in new OrdersVariables{

    OrdersUtils.isASaleArticle(LIST_INDICATORS_SALES_ARTICLE, INDICATOR_SALE_1) shouldBe true
    OrdersUtils.isASaleArticle(LIST_INDICATORS_SALES_ARTICLE, INDICATOR_SALE_2) shouldBe true
    OrdersUtils.isASaleArticle(LIST_INDICATORS_SALES_ARTICLE, "Another") shouldBe false
    OrdersUtils.isASaleArticle(LIST_INDICATORS_SALES_ARTICLE, null) shouldBe true

  }

  "isBoxOrPrepack " must
    " return true when the input article is a PREPACK or BOX" taggedAs (UnitTestTag) in new OrdersVariables{

    OrdersUtils.isBoxOrPrepack(articleTypeAndCategoryPREPACK_BOX_1, defaultConfig.getBoxOrPrepackArticleList) shouldBe true
    OrdersUtils.isBoxOrPrepack(articleTypeAndCategoryPREPACK_BOX_2, defaultConfig.getBoxOrPrepackArticleList) shouldBe true
    OrdersUtils.isBoxOrPrepack(articleTypeAndCategoryWrong1, defaultConfig.getBoxOrPrepackArticleList) shouldBe false
    OrdersUtils.isBoxOrPrepack(articleTypeAndCategoryWrong2, defaultConfig.getBoxOrPrepackArticleList) shouldBe false
    OrdersUtils.isBoxOrPrepack(articleTypeAndCategoryWrong3, defaultConfig.getBoxOrPrepackArticleList) shouldBe false
    OrdersUtils.isBoxOrPrepack(articleTypeAndCategoryNull1, defaultConfig.getBoxOrPrepackArticleList) shouldBe false
    OrdersUtils.isBoxOrPrepack(articleTypeAndCategoryUNIQUE_SALE_ARTICLE_1, defaultConfig.getBoxOrPrepackArticleList) shouldBe false
  }

  it must
    " return true when the input article is a PREPACK or BOX with empty list in configuration" taggedAs (UnitTestTag) in new OrdersVariables{

    val newConf = defaultConfig.setBoxOrPrepackArticleList(List.empty[ArticleTypeAndCategory])

    OrdersUtils.isBoxOrPrepack(articleTypeAndCategoryPREPACK_BOX_1, newConf.getBoxOrPrepackArticleList) shouldBe false
    OrdersUtils.isBoxOrPrepack(articleTypeAndCategoryPREPACK_BOX_2, newConf.getBoxOrPrepackArticleList) shouldBe false
    OrdersUtils.isBoxOrPrepack(articleTypeAndCategoryWrong1, newConf.getBoxOrPrepackArticleList) shouldBe false
    OrdersUtils.isBoxOrPrepack(articleTypeAndCategoryWrong2, newConf.getBoxOrPrepackArticleList) shouldBe false
    OrdersUtils.isBoxOrPrepack(articleTypeAndCategoryWrong3, newConf.getBoxOrPrepackArticleList) shouldBe false
    OrdersUtils.isBoxOrPrepack(articleTypeAndCategoryNull1, newConf.getBoxOrPrepackArticleList) shouldBe false
    OrdersUtils.isBoxOrPrepack(articleTypeAndCategoryUNIQUE_SALE_ARTICLE_1, newConf.getBoxOrPrepackArticleList) shouldBe false
  }


  "isAPuchaseArticleWithUniqueSaleArticle " must
    " return true when the input article is a purchase article with a unique sale article" taggedAs (UnitTestTag) in new OrdersVariables{

    OrdersUtils.isAPuchaseArticleWithUniqueSaleArticle(articleTypeAndCategoryPREPACK_BOX_1, "AB", defaultConfig.getPuchaseArticleWithUniqueSaleArticleList) shouldBe false
    OrdersUtils.isAPuchaseArticleWithUniqueSaleArticle(articleTypeAndCategoryPREPACK_BOX_2, "AB", defaultConfig.getPuchaseArticleWithUniqueSaleArticleList) shouldBe false
    OrdersUtils.isAPuchaseArticleWithUniqueSaleArticle(articleTypeAndCategoryWrong1, "AB", defaultConfig.getPuchaseArticleWithUniqueSaleArticleList) shouldBe false
    OrdersUtils.isAPuchaseArticleWithUniqueSaleArticle(articleTypeAndCategoryWrong2, "AB", defaultConfig.getPuchaseArticleWithUniqueSaleArticleList) shouldBe false
    OrdersUtils.isAPuchaseArticleWithUniqueSaleArticle(articleTypeAndCategoryWrong3, "AB", defaultConfig.getPuchaseArticleWithUniqueSaleArticleList) shouldBe false
    OrdersUtils.isAPuchaseArticleWithUniqueSaleArticle(articleTypeAndCategoryNull1, "AB", defaultConfig.getPuchaseArticleWithUniqueSaleArticleList) shouldBe false
    OrdersUtils.isAPuchaseArticleWithUniqueSaleArticle(articleTypeAndCategoryUNIQUE_SALE_ARTICLE_1, "AB", defaultConfig.getPuchaseArticleWithUniqueSaleArticleList) shouldBe true

  }

  it must
    " return true when the input article is a purchase article with a unique sale article with empty list in configuration" taggedAs (UnitTestTag) in new OrdersVariables{

    val newConf = defaultConfig.setPuchaseArticleWithUniqueSaleArticleList(List.empty[ArticleTypeAndCategory])

    OrdersUtils.isAPuchaseArticleWithUniqueSaleArticle(articleTypeAndCategoryPREPACK_BOX_1, "AB", newConf.getPuchaseArticleWithUniqueSaleArticleList) shouldBe false
    OrdersUtils.isAPuchaseArticleWithUniqueSaleArticle(articleTypeAndCategoryPREPACK_BOX_2, "AB", newConf.getPuchaseArticleWithUniqueSaleArticleList) shouldBe false
    OrdersUtils.isAPuchaseArticleWithUniqueSaleArticle(articleTypeAndCategoryWrong1, "AB", newConf.getPuchaseArticleWithUniqueSaleArticleList) shouldBe false
    OrdersUtils.isAPuchaseArticleWithUniqueSaleArticle(articleTypeAndCategoryWrong2, "AB", newConf.getPuchaseArticleWithUniqueSaleArticleList) shouldBe false
    OrdersUtils.isAPuchaseArticleWithUniqueSaleArticle(articleTypeAndCategoryWrong3, "AB", newConf.getPuchaseArticleWithUniqueSaleArticleList) shouldBe false
    OrdersUtils.isAPuchaseArticleWithUniqueSaleArticle(articleTypeAndCategoryNull1, "AB", newConf.getPuchaseArticleWithUniqueSaleArticleList) shouldBe false
    OrdersUtils.isAPuchaseArticleWithUniqueSaleArticle(articleTypeAndCategoryUNIQUE_SALE_ARTICLE_1, "AB", newConf.getPuchaseArticleWithUniqueSaleArticleList) shouldBe false

  }

  it must
    " return false when we have a null salesParentArticle" taggedAs (UnitTestTag) in new OrdersVariables{

    OrdersUtils.isAPuchaseArticleWithUniqueSaleArticle(articleTypeAndCategoryUNIQUE_SALE_ARTICLE_1, null, defaultConfig.getPuchaseArticleWithUniqueSaleArticleList) shouldBe false
    OrdersUtils.isAPuchaseArticleWithUniqueSaleArticle(null, null, defaultConfig.getPuchaseArticleWithUniqueSaleArticleList) shouldBe false

  }

  "isASaleArticleOrAPuchaseArticleWithUniqueSaleArticle " must
    " return true when the input article is a sale article or a purchase article with a unique sale article" taggedAs (UnitTestTag) in new OrdersVariables{

    OrdersUtils.isASaleArticleOrAPuchaseArticleWithUniqueSaleArticle(
      LIST_INDICATORS_SALES_ARTICLE,
      INDICATOR_SALE_1,
      articleTypeAndCategoryPREPACK_BOX_1,
      "AB",
      defaultConfig.getPuchaseArticleWithUniqueSaleArticleList
    ) shouldBe true
    OrdersUtils.isASaleArticleOrAPuchaseArticleWithUniqueSaleArticle(
      LIST_INDICATORS_SALES_ARTICLE,
      INDICATOR_SALE_2,
      articleTypeAndCategoryPREPACK_BOX_2,
      "AB",
      defaultConfig.getPuchaseArticleWithUniqueSaleArticleList
    ) shouldBe true
    OrdersUtils.isASaleArticleOrAPuchaseArticleWithUniqueSaleArticle(
      LIST_INDICATORS_SALES_ARTICLE,
      null,
      articleTypeAndCategoryPREPACK_BOX_2,
      "AB",
      defaultConfig.getPuchaseArticleWithUniqueSaleArticleList
    ) shouldBe true
    OrdersUtils.isASaleArticleOrAPuchaseArticleWithUniqueSaleArticle(
      LIST_INDICATORS_SALES_ARTICLE,
      "Another",
      articleTypeAndCategoryPREPACK_BOX_2,
      "AB",
      defaultConfig.getPuchaseArticleWithUniqueSaleArticleList
    ) shouldBe false
    OrdersUtils.isASaleArticleOrAPuchaseArticleWithUniqueSaleArticle(
      LIST_INDICATORS_SALES_ARTICLE,
      "Another",
      articleTypeAndCategoryUNIQUE_SALE_ARTICLE_1,
      "AB",
      defaultConfig.getPuchaseArticleWithUniqueSaleArticleList
    ) shouldBe true
    OrdersUtils.isASaleArticleOrAPuchaseArticleWithUniqueSaleArticle(
      LIST_INDICATORS_SALES_ARTICLE,
      "Another",
      articleTypeAndCategoryUNIQUE_SALE_ARTICLE_1,
      null,
      defaultConfig.getPuchaseArticleWithUniqueSaleArticleList
    ) shouldBe false
    OrdersUtils.isASaleArticleOrAPuchaseArticleWithUniqueSaleArticle(
      LIST_INDICATORS_SALES_ARTICLE,
      INDICATOR_SALE_1,
      articleTypeAndCategoryUNIQUE_SALE_ARTICLE_1,
      "AB",
      defaultConfig.getPuchaseArticleWithUniqueSaleArticleList
    ) shouldBe true
    OrdersUtils.isASaleArticleOrAPuchaseArticleWithUniqueSaleArticle(
      LIST_INDICATORS_SALES_ARTICLE,
      INDICATOR_SALE_2,
      articleTypeAndCategoryUNIQUE_SALE_ARTICLE_1,
      "AB",
      defaultConfig.getPuchaseArticleWithUniqueSaleArticleList
    ) shouldBe true
    OrdersUtils.isASaleArticleOrAPuchaseArticleWithUniqueSaleArticle(
      LIST_INDICATORS_SALES_ARTICLE,
      null,
      articleTypeAndCategoryUNIQUE_SALE_ARTICLE_1,
      "AB",
      defaultConfig.getPuchaseArticleWithUniqueSaleArticleList
    ) shouldBe true
    OrdersUtils.isASaleArticleOrAPuchaseArticleWithUniqueSaleArticle(
      LIST_INDICATORS_SALES_ARTICLE,
      INDICATOR_SALE_1,
      articleTypeAndCategoryUNIQUE_SALE_ARTICLE_1,
      null,
      defaultConfig.getPuchaseArticleWithUniqueSaleArticleList
    ) shouldBe true
  }

}
