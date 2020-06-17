package com.training.bigdata.omnichannel.customerOrderReservation.events

/**
  * Income Events from Kafka
  */
object CustomerOrderReservationEvent {
  
  type StringNullable = Option[String]

  trait Events extends Serializable

  case class ClickCollect(
    storeName: StringNullable,
    storeId: StringNullable,
    rpmorderNumber: StringNullable
  )

  case class DeliveryInformation(
    originalDeliveryDate: StringNullable,
    deliveryDate: StringNullable,
    deliveryFromHour: StringNullable,
    deliverytoHour: StringNullable,
    deliveryTimeSlotId: StringNullable
  )

  case class LogisticFood(
    salePointName: StringNullable,
    salePointId: StringNullable,
    regionDesc: StringNullable,
    regionId: StringNullable,
    zoneDesc: StringNullable,
    subzoneDesc: StringNullable,
    subzoneCode: StringNullable,
    zoneId: StringNullable,
    routeId: StringNullable
  )

  case class Address(
    country: StringNullable,
    surname: StringNullable,
    name: StringNullable,
    phoneNumber: StringNullable,
    block: StringNullable,
    floor: StringNullable,
    door: StringNullable,
    province: StringNullable,
    city: StringNullable,
    streetName: StringNullable,
    streetNumber: StringNullable,
    streetType: StringNullable,
    stair: StringNullable,
    neighborhood: StringNullable,
    zipCode: StringNullable,
    transporterInfo: StringNullable,
    censusTract: StringNullable,
    coordX: StringNullable,
    coordY: StringNullable,
    reliabilityIndex: StringNullable,
    normalizationStatus: Option[Boolean]
  )

  case class Adjustment(
    id: String,
    adjustmentType: String,
    promotionType: StringNullable,
    amount: Option[Double],
    dofoCode: StringNullable,
    description: StringNullable,
    promoId: StringNullable,
    couponDesc: StringNullable,
    couponId: StringNullable,
    applicableSite: StringNullable,
    generatedCoupon: StringNullable,
    accumulationDate: StringNullable,
    employeeDiscountPercent: Option[Double],
    endRedentionDate: StringNullable,
    accumulatedAmount: Option[Double],
    spentAmount: Option[Double],
    customerId: StringNullable,
    name: StringNullable,
    sequencial: StringNullable,
    taxRecovery: Option[Boolean],
    provision: Option[Double],
    redemptionRatio: Option[Double],
    applicationScope: StringNullable
  )

  case class ShippingPriceInfo(
    amount: Double,
    coste1Type: StringNullable,
    coste1: Option[Double],
    totalAdjustment: Option[Double],
    amountNoDiscounts: Option[Double],
    adjustments: Option[List[Adjustment]]
  )

  case class ShippingGroup(
    daysToDeliver: StringNullable,
    shippingMethod: String,
    shippingGroupId: String,
    rawShipping: Option[Double],
    clientCourierId: StringNullable,
    shippingGroupType: StringNullable,
    glcreceivedDate: StringNullable,
    state: String,
    glcorderNumber: StringNullable,
    pickingDate: StringNullable,
    clickCollect: Option[ClickCollect],
    deliveryInformation: Option[DeliveryInformation],
    logisticFood: Option[LogisticFood],
    address: Address,
    shippingPriceInfo: ShippingPriceInfo
  )

  case class Volumetry(
    width: Option[Double],
    high: Option[Double],
    weight: Option[Double],
    length: Option[Double]
  )

  case class CommerceItemPriceInfo(
    amountNoDiscounts: Option[Double],
    iva: Double,
    ruleId: StringNullable,
    costPrice: Option[Double],
    totalAdjustment: Option[Double],
    sellerId: StringNullable,
    sellerName: StringNullable,
    amount: Double,
    salePrice: Option[Double],
    listPrice: Double,
    incentive: StringNullable,
    /* TODO: OJO Sin definir en el AVRO*/
    adjustments: Option[List[Adjustment]]
  )

  case class CommerceItem(
    paymentGroupId: StringNullable,
    color: StringNullable,
    quantity: Int,
    productId: StringNullable,
    shippingGroupId: StringNullable,
    skuId: String,
    ean13: StringNullable,
    smsSizeColor: StringNullable,
    providerLdp: StringNullable,
    manufacturerCode: StringNullable,
    legalAdvertisementAcCheck: Option[Boolean],
    name: StringNullable,
    size: StringNullable,
    quantityDelivered: Option[Int],
    grossWeight: Option[Double],
    weight: Option[Double],
    variableWeight: Option[Boolean],
    smsId: StringNullable,
    unitOfMeasure: StringNullable,
    typeOfCut: StringNullable,
    missed: Option[Boolean],
    substitute: Option[Boolean],
    itemReplace: StringNullable,
    foodLine: StringNullable,
    marketplace: Option[Boolean],
    locationId: StringNullable,
    webDescription: StringNullable,
    brand: StringNullable,
    container: StringNullable,
    section: StringNullable,
    family: StringNullable,
    subfamily: StringNullable,
    volumetry: Volumetry,
    commerceItemPriceInfo: CommerceItemPriceInfo
  )

  case class PaymentGroup(
    alias: StringNullable,
    paymentGroupId: StringNullable,
    omsOrderId: StringNullable,
    creditCardType: StringNullable,
    amount: Double,
    paymentMethod: StringNullable,
    docIdType: StringNullable,
    docId: StringNullable,
    paymentStatus: StringNullable,
    creditCardLastDigits: StringNullable,
    descriptiveCreditCardType: StringNullable,
    paymentGroupType: StringNullable,
    payId: StringNullable,
    pspId: StringNullable,
    authorizationStatus: StringNullable,
    paymentDate: StringNullable,
    /* TODO: OJO Sin definir en el AVRO*/
    address: Address,
    securePayment: Option[Boolean],
    funding: Option[Funding]
  )

  case class Funding(
    fundedAmount: Option[Double],
    manageFee: Option[Double],
    description: StringNullable,
    TIN: Option[Double],
    TAE: Option[Double],
    incentiveFunding: StringNullable,
    months: Option[Int]
  )

  case class OrderCustomerInfo(
    customerDoc: String,
    customerId: StringNullable,
    customerName: String,
    customerLastName: String,
    customerEmail: String,
    customerDocType: StringNullable,
    customerTelephoneNumber: StringNullable,
    bussinessName: StringNullable,
    fidelizationCard: StringNullable
  )

  case class OrderPriceInfo(
    clubAccumulated: Option[Double],
    amount: Double,
    sellerName: StringNullable,
    sellerId: StringNullable,
    shippingAmount: Double,
    totalAmount: Double,
    amountNoDiscounts: Option[Double],
    applicationId: StringNullable,
    /* TODO: OJO Sin definir en el AVRO*/
    adjustments: Option[List[Adjustment]]
  )

  case class FlatRate(
    duration: Option[Double],
    endDate: StringNullable,
    flatPrice: Option[Double]
  )

  case class SpecialMark(
    category: StringNullable,
    specialMarkType: StringNullable,
    motive: StringNullable
  )

  case class Order(
    shippingGroups: List[ShippingGroup],
    commerceItems: List[CommerceItem],
    paymentGroups: List[PaymentGroup],
    orderCustomerInfo: OrderCustomerInfo,
    orderPriceInfo: OrderPriceInfo,
    siteId: String,
    creationDate: String,
    lastModifiedDate: String,
    submittedDate: String,
    originOfOrder: StringNullable,
    orderType: StringNullable,
    logistic: StringNullable,
    isNewPreparation: Option[Boolean],
    isNewOrder: Option[Boolean],
    migrated: Option[Boolean],
    state: String,
    miraklId: StringNullable,
    miraklStatus: StringNullable,
    omsOrderId: String,
    isAnonymous: Boolean,
    parentOrderId: StringNullable,
    salesmanId: StringNullable,
    mallId: StringNullable,
    billingStatus: Option[Int],
    flatRate: Option[FlatRate],
    specialMark: Option[SpecialMark]
  ) extends Events
}






