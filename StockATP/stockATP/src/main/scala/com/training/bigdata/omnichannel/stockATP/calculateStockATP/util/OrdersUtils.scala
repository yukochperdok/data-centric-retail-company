package com.training.bigdata.omnichannel.stockATP.calculateStockATP.util

import com.training.bigdata.omnichannel.stockATP.common.util.ArticleTypeAndCategory

object OrdersUtils{

  def isOfTypeAndCategoryArticle(
    articleTypeAndCategory: ArticleTypeAndCategory,
    articleTypesAndCategories: List[ArticleTypeAndCategory]): Boolean = {

      articleTypesAndCategories.exists(item =>
        articleTypeAndCategory.articleType == item.articleType &&
          articleTypeAndCategory.category == item.category
      )
  }

  val isASaleArticle: (=>List[String], String) => Boolean = {
    (saleArticlesList, salePurchaseIndicator) => salePurchaseIndicator == null || saleArticlesList.contains(salePurchaseIndicator)
  }

  val isBoxOrPrepack: (ArticleTypeAndCategory, List[ArticleTypeAndCategory]) => Boolean = {
    (articleTypeAndCategory, boxAndPrepackTypeAndCategoryList) =>
      isOfTypeAndCategoryArticle(
        articleTypeAndCategory,
        boxAndPrepackTypeAndCategoryList
      )
  }

  val isAPuchaseArticleWithUniqueSaleArticle: (=>ArticleTypeAndCategory, String, List[ArticleTypeAndCategory]) => Boolean = {
    (articleTypeAndCategory, salesParentArticle, purchaseArticleTypeAndCategoryWithUniqueSaleArticleList) =>
      salesParentArticle != null &&
        isOfTypeAndCategoryArticle(
          articleTypeAndCategory,
          purchaseArticleTypeAndCategoryWithUniqueSaleArticleList
        )
  }

  val isASaleArticleOrAPuchaseArticleWithUniqueSaleArticle:
    (List[String], String, ArticleTypeAndCategory, String, List[ArticleTypeAndCategory]) => Boolean = {

    (saleArticlesList, salePurchaseIndicator, articleTypeAndCategory, salesParentArticle, purchaseArticleTypeAndCategoryWithUniqueSaleArticleList) =>
      isASaleArticle(saleArticlesList, salePurchaseIndicator) ||
        isAPuchaseArticleWithUniqueSaleArticle(articleTypeAndCategory, salesParentArticle, purchaseArticleTypeAndCategoryWithUniqueSaleArticleList)
  }
}
