const getLookupWithCondition = ({
  from,
  localField,
  localFieldArr,
  foreignField,
  as,
  extraValidation = [],
  extraFields = {},
  useOrForExtra,
  deleted,
  project = {},
  sort = {}
}) => {
  const deletedCheck = []
  if (!deleted) {
    deletedCheck.push({
      $eq: [{ $type: '$deletedOn' }, 'missing']
    })
  }

  const pipeline = []
  pipeline.push({
    $match: {
      $expr: {
        $and: [
          {
            [useOrForExtra ? '$or' : '$and']: [
              {
                [localField ? '$eq' : '$in']: ['$' + foreignField, '$$itemId']
              },
              ...extraValidation
            ]
          },
          ...deletedCheck
        ]
      }
    }
  })

  if (Object.keys(project).length > 0) {
    pipeline.push({
      $project: project
    })
  }
  if (Object.keys(sort).length > 0) {
    pipeline.push({
      $sort: sort
    })
  }

  return {
    $lookup: {
      from,
      let: {
        itemId: localField ? '$' + localField : localFieldArr,
        ...extraFields
      },
      pipeline,
      as
    }
  }
}
const pushAttribute = ({
  from,
  includeChild,
  deleted,
  array = [],
  project = {}
}) => {
  if (from === 'itemattributevalues') {
    array.push(
      getLookupWithCondition({
        from: 'it',
        localField: 'parentId',
        foreignField: 'parentId',
        as: 'iteAttInh',
        deleted
      }),
      getLookupWithCondition({
        from,
        ...getExtendedObj(
          includeChild,
          { localFieldArr: { $concatArrays: ['$childrenIds', ['$_id']] } },
          { localField: '_id' }
        ),
        foreignField: 'itemId',
        as: from,
        extraFields: {
          parentId: '$parentId',
          attributeIds: getReducedIds('$iteAttInh', 'attributeId')
        },
        extraValidation: [
          {
            $and: [
              { $eq: ['$itemId', '$$parentId'] },
              { $in: ['$attributeId', '$$attributeIds'] }
            ]
          }
        ],
        useOrForExtra: true,
        deleted
      })
    )
    project.iteAttInh = 0
  } else if (from === 'nodeattributevalues') {
    array.push(
      getLookupWithCondition({
        from,
        ...getExtendedObj(
          includeChild,
          { localFieldArr: { $concatArrays: ['$childrenIds', ['$_id']] } },
          { localField: '_id' }
        ),
        foreignField: 'nodeId',
        as: from,
        deleted
      })
    )
  } else if (from === 'vendorattributevalues') {
    array.push(
      getLookupWithCondition({
        from: from,
        localField: '_id',
        foreignField: 'vendorId',
        as: 'vendorattributevalues',
        useOrForExtra: true
      })
    )
  }
  const path = '$' + from
  project[from] = 0
  array.push(
    getLookupWithCondition({
      from: 'attributeviews',
      localFieldArr: getReducedIds(path, 'attributeId'),
      foreignField: '_id',
      as: 'attr',
      deleted
    })
  )

  array.push(
    getLookupWithCondition({
      from: 'attributevalues',
      localFieldArr: getReducedIds(path, 'attributeValueId'),
      foreignField: '_id',
      as: 'attrValues',
      deleted
    })
  )
  project.attrValues = 0
  project.attr = 0
  array.push({
    $addFields: {
      attributes: {
        $function: {
          body: `function (attr, entityAttrVal, attrValues, _id, parentId) {
            const attrObj = {};
            for (const el of attr) {
              attrObj[el._id] = el;
            }
            const attrValuesObj = {};
            for (const el of attrValues) {
              attrValuesObj[el._id] = el;
            }
            const array = [];
            let mappedValues = {};
            for (const el of entityAttrVal) {
              if (attrValuesObj[el.attributeValueId]) {
                const temp = {
                  ...attrValuesObj[el.attributeValueId],
                  ...el,
                  ...attrObj[el.attributeId] || {},
                };
                temp.id = temp._id;
                delete temp._id;
                delete temp.__v;
                if(temp.itemId){
                  if (temp.mapping && temp.itemId + '' === _id + '' && mappedValues[temp.mapping] === undefined) {
                    mappedValues[temp.mapping] = temp.valueRaw;
                  }
                  if (temp.mapping && temp.itemId + '' === parentId + '') {
                    mappedValues[temp.mapping] = temp.valueRaw;
                  }}
                else if (temp.vendorId && temp.mapping && temp.vendorId + '' === _id + '' && mappedValues[temp.mapping] === undefined) {
                  mappedValues[temp.mapping] = temp.valueRaw;
                }
                array.push(temp);
              }
            }
            return { array, mappedValues };
          }`,
          args: ['$attr', path, '$attrValues', '$_id', '$parentId'],
          lang: 'js'
        }
      }
    }
  })
  return array
}
