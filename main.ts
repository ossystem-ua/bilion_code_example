import {Getter, inject} from '@loopback/core';
import {BelongsToAccessor, Filter, HasManyThroughRepositoryFactory, repository} from '@loopback/repository';
import {BuildingRepository, HoaUnitTypeServiceRepository} from '.';
import {PostgresqlDataSource} from '../datasources';
import {ConstantsBindings} from '../keys';
import {Building, Hoa, HoaRelations, HoaType, HoaUnitType, Service, UnitType} from '../models';
import {BaseRepo, CustomError} from '../types';
import {ErrorCodes} from '../types/enumerations';
import {DataResponse} from '../types/interfaces';
import {HoaTypeRepository} from './hoa-type.repository';
import {HoaUnitTypeRepository} from './hoa-unit-type.repository';
import {ServiceRepository} from './service.repository';
import {UnitTypeRepository} from './unit-type.repository';

export class HoaRepository extends BaseRepo<Hoa, typeof Hoa.prototype.hoa_id, HoaRelations> {
  public readonly hoaType: BelongsToAccessor<HoaType, typeof Hoa.prototype.id>;

  public readonly unitTypes: HasManyThroughRepositoryFactory<
    UnitType,
    typeof UnitType.prototype.unit_type_id,
    HoaUnitType,
    typeof Hoa.prototype.hoa_id
  >;

  constructor(
    @inject('datasources.postgresql') dataSource: PostgresqlDataSource,
    @inject(ConstantsBindings.CURRENT_LANGUAGE) private lang: string,
    @repository.getter('HoaTypeRepository')
    protected hoaTypeRepositoryGetter: Getter<HoaTypeRepository>,

    @repository.getter('UnitTypeRepository')
    protected unitTypesRepositoryGetter: Getter<UnitTypeRepository>,
    @repository.getter('ServiceRepository')
    protected serviceRepositoryGetter: Getter<ServiceRepository>,
    @repository.getter('HoaUnitTypeRepository') protected hoaUnitTypeRepositoryGetter: Getter<HoaUnitTypeRepository>,
    @repository.getter('HoaUnitTypeServiceRepository')
    protected hoaUnitTypeServiceRepositoryGetter: Getter<HoaUnitTypeServiceRepository>,
    @repository.getter('UnitTypeRepository') protected unitTypeRepositoryGetter: Getter<UnitTypeRepository>,

    @repository.getter('BuildingRepository') protected buildingRepositoryGetter: Getter<BuildingRepository>,
  ) {
    super(Hoa, dataSource);
    this.unitTypes = this.createHasManyThroughRepositoryFactoryFor(
      'unitTypes',
      unitTypeRepositoryGetter,
      hoaUnitTypeRepositoryGetter,
    );
    this.registerInclusionResolver('unitTypes', this.unitTypes.inclusionResolver);
    this.hoaType = this.createBelongsToAccessorFor('hoaType', hoaTypeRepositoryGetter);
    this.registerInclusionResolver('hoaType', this.hoaType.inclusionResolver);
  }

  async findHoas(filter?: Filter<Hoa>): Promise<DataResponse<Hoa>> {
    let columns: string = this.dataSource.connector?.buildColumnNames(this.entityClass.definition.name, filter);

    const additionalFields = ['hoa_type', 'type_name', 'cityname'];

    columns = columns
      .replace(/"/g, '')
      .split(',')
      .filter(col => !additionalFields.includes(col))
      .map(h => `h.${h}`)
      .join(',');

    const query = `
      SELECT * FROM (
        SELECT ${columns},
          CONCAT (hoa_abbr.text, ' ', '"', h.name, '"') AS type_name,
          t.text AS hoa_type,
          city.text AS cityname
        FROM hoa h
        LEFT JOIN hoa_type ht ON h.hoa_type_id = ht.hoa_type_id
        LEFT JOIN translation hoa_abbr ON hoa_abbr.group_id = ht.abbr_id AND hoa_abbr.lang = '${this.lang}'
        LEFT JOIN translation city ON city.group_id = h.city_id and city.lang = '${this.lang}'
        LEFT JOIN translation t ON t.group_id = h.hoa_type_id AND t.lang = '${this.lang}'
      ) hoas
    `;

    return this.findWithPaginator(filter, {query, additionalFields});
  }

  async findHoa(id: string, filter?: Filter<Hoa>): Promise<Hoa> {
    filter = {
      ...filter,
      where: {
        ...filter?.where,
        hoa_id: id,
      },
    };
    return (await this.findHoas(filter)).data[0];
  }

  async findAllForHoa(
    id?: string,
    filter?: Filter<Building>,
    hoaTypeFilter?: Filter<HoaType>,
    unitTypeFilter?: Filter<UnitType>,
    ServiceFilter?: Filter<Service>,
  ): Promise<object> {
    const hoaTypeRepository = await this.hoaTypeRepositoryGetter();
    const unitTypesRepository = await this.unitTypesRepositoryGetter();
    const serviceRepository = await this.serviceRepositoryGetter();
    const hoaUnitTypeRepository = await this.hoaUnitTypeRepositoryGetter();
    const hoaUnitTypeServiceRepository = await this.hoaUnitTypeServiceRepositoryGetter();
    const hoaTypes = (await hoaTypeRepository.findHoaTypes(hoaTypeFilter)).data;
    const unitTypes = (await unitTypesRepository.findUnitTypes(unitTypeFilter)).data;
    const services = (await serviceRepository.findServices(ServiceFilter)).data;

    const allForHoa: {
      hoaTypes: HoaType[];
      unitTypes: UnitType[];
      services: Service[];
      hoa?: Hoa;
      hoaUT?: {};
    } = {
      hoaTypes,
      unitTypes,
      services,
    };

    if (id) {
      let hoaUT = {};
      const hoaUnitTypes = await hoaUnitTypeRepository.find({
        where: {
          hoa_id: {
            eq: id,
          },
        },
      });

      if (hoaUnitTypes.length) {
        for (const hoaUnitType of hoaUnitTypes) {
          const arr: Array<string> = [];
          const hoaUnitTypeServices = await hoaUnitTypeServiceRepository.find({
            where: {
              hoa_unit_type_id: {
                eq: hoaUnitType.hoa_unit_type_id,
              },
            },
          });
          hoaUnitTypeServices.forEach(hoaUTService => {
            arr.push(hoaUTService.service_id);
          });
          hoaUT = {...hoaUT, [hoaUnitType.unit_type_id]: arr};
        }
      }

      filter = {
        ...filter,
        where: {
          ...filter?.where,
          hoa_id: id,
        },
      };
      try {
        const hoa = (await this.findHoas(filter)).data[0];
        allForHoa.hoaUT = hoaUT;
        allForHoa.hoa = hoa;
      } catch (err) {
        console.log(err);
      }
    }

    return allForHoa;
  }

  async createHoa(data: Omit<Hoa, 'updated' | 'created'>): Promise<Hoa> {
    const transaction = await this.beginTransaction();
    const unitTypes = data.unitTypes;

    delete data.unitTypes;
    try {
      const hoa = await this.create(data, {transaction});
      const hoaUnitTypeRepository = await this.hoaUnitTypeRepositoryGetter();
      const hoaUnitTypeServiceRepository = await this.hoaUnitTypeServiceRepositoryGetter();
      for (const unitType in unitTypes) {
        const hoaUnitType = {
          unit_type_id: unitType,
          hoa_id: hoa.hoa_id,
        };
        const newHoaUnitType = await hoaUnitTypeRepository.create(hoaUnitType, {transaction});
        for (const service of unitTypes[unitType]) {
          const unitTypeService = {
            hoa_unit_type_id: newHoaUnitType.hoa_unit_type_id,
            service_id: service,
          };
          await hoaUnitTypeServiceRepository.create(unitTypeService, {transaction});
        }
      }
      await transaction.commit();
      return await this.findById(hoa.hoa_id);
    } catch (error) {
      await transaction.rollback();
      throw error;
    }
  }

  async updateHoa(id: string, data: Omit<Hoa, 'updated'>): Promise<Hoa> {
    const hoaUnitTypeRepository = await this.hoaUnitTypeRepositoryGetter();
    const hoaUnitTypeServiceRepository = await this.hoaUnitTypeServiceRepositoryGetter();
    const transaction = await this.beginTransaction();
    const unitTypes = data.unitTypes;
    const unitTypesFromClientIds = Object.keys(unitTypes);
    delete data.unitTypes;
    let unitTypesIdsToCreate: Array<string> = [];
    let unitTypesIDsToDelete: Array<string> = [];
    let unitTypesIDsToUpdate: Array<string> = [];
    let hoaUnitTypesIDsToDelete: Array<string | undefined> = [];
    let hoaUnitTypesToUpdate: Array<HoaUnitType | undefined> = [];
    try {
      const currentHoaUnitTypes = await hoaUnitTypeRepository.find({
        where: {
          hoa_id: {
            eq: id,
          },
        },
      });

      const currentUnitTypesIds = currentHoaUnitTypes.map(hut => hut.unit_type_id);
      unitTypesIdsToCreate = unitTypesFromClientIds.filter(unitTypeId => !currentUnitTypesIds.includes(unitTypeId));

      unitTypesIDsToDelete = currentUnitTypesIds.filter(unitTypeId => !unitTypesFromClientIds.includes(unitTypeId));

      const changedUTIds = [...unitTypesIdsToCreate, ...unitTypesIDsToDelete];
      hoaUnitTypesIDsToDelete = unitTypesIDsToDelete.map(
        utID => currentHoaUnitTypes.find(item => item.unit_type_id === utID)?.hoa_unit_type_id,
      );

      unitTypesIDsToUpdate = unitTypesFromClientIds.filter(hoUT => !changedUTIds.includes(hoUT));
      hoaUnitTypesToUpdate = unitTypesIDsToUpdate.map(utID =>
        currentHoaUnitTypes.find(item => item.unit_type_id === utID),
      );

      // CREATING NEW UNIT TYPES
      if (unitTypesIdsToCreate.length) {
        for (const unitTypeId of unitTypesIdsToCreate) {
          const hoaUnitType = {
            unit_type_id: unitTypeId,
            hoa_id: id,
          };
          const newHoaUnitType = await hoaUnitTypeRepository.create(hoaUnitType, {transaction});
          for (const service of unitTypes[unitTypeId]) {
            const unitTypeService = {
              hoa_unit_type_id: newHoaUnitType.hoa_unit_type_id,
              service_id: service,
            };
            await hoaUnitTypeServiceRepository.create(unitTypeService, {transaction});
          }
        }
      }
      // DELETING NEW UNIT TYPES
      if (hoaUnitTypesIDsToDelete.length) {
        for (const hoaUnitTypeId of hoaUnitTypesIDsToDelete) {
          if (hoaUnitTypeId) {
            await hoaUnitTypeRepository.deleteById(hoaUnitTypeId, {transaction});
            for (const service of unitTypes[hoaUnitTypeId]) {
              await hoaUnitTypeServiceRepository.deleteById(service, {transaction});
            }
          }
        }
      }
      // UPDATING  SERVICES  IN HOA_UNIT_TYPE_SERVICES
      if (hoaUnitTypesToUpdate.length) {
        for (const hoaUnitType of hoaUnitTypesToUpdate) {
          if (hoaUnitType) {
            let servicesIdsToCreate: Array<string> = [];
            let servicesIDsToDelete: Array<string> = [];
            const currentHoaUnitTypeServices = await hoaUnitTypeServiceRepository.find({
              where: {
                hoa_unit_type_id: {
                  eq: hoaUnitType.hoa_unit_type_id,
                },
              },
            });

            const prevServicesId = unitTypes[hoaUnitType.unit_type_id];
            const currentServicesIds = currentHoaUnitTypeServices.map(item => item.service_id);
            servicesIdsToCreate = prevServicesId.filter(
              (hoaUTService: string) => !currentServicesIds.includes(hoaUTService),
            );
            servicesIDsToDelete = currentServicesIds.filter(hoaUTService => !prevServicesId.includes(hoaUTService));

            // CREATING HOAUNITTYPESERVICE
            for (const servicesId of servicesIdsToCreate) {
              const unitTypeService = {
                hoa_unit_type_id: hoaUnitType.hoa_unit_type_id,
                service_id: servicesId,
              };
              await hoaUnitTypeServiceRepository.create(unitTypeService, {transaction});
            }
            // DELETING HOA_UNIT_TYPE_SERVICE
            const hoaUnitTypeServiceIdsToDelete = servicesIDsToDelete.map(
              SID => currentHoaUnitTypeServices.find(item => item.service_id === SID)?.hoa_unit_type_service_id,
            );
            for (const hoaUTServiceId of hoaUnitTypeServiceIdsToDelete) {
              await hoaUnitTypeServiceRepository.deleteById(hoaUTServiceId, {transaction});
            }
          }
        }
      }
      await this.updateById(id, data, {transaction});
      await transaction.commit();
    } catch (error) {
      await transaction.rollback();
      throw error;
    }

    return this.findById(id);
  }

  async deletingHoa(hoaIdList: Array<string>): Promise<void> {
    const transaction = await this.beginTransaction();
    const hoaUnitTypeRepository = await this.hoaUnitTypeRepositoryGetter();
    const hoaUnitTypeServiceRepository = await this.hoaUnitTypeServiceRepositoryGetter();
    const buildingRepository = await this.buildingRepositoryGetter();

    // checking if hoa has linked buildings
    const buildings = await buildingRepository.find({
      where: {
        hoa_id: {inq: hoaIdList},
      },
    });

    let acceptedIdsToDelete: Array<string> = hoaIdList;
    let rejectedIdsToDelete: Set<string> = new Set();

    if (buildings?.length) {
      rejectedIdsToDelete = new Set(buildings.map(elem => elem.hoa_id));
      acceptedIdsToDelete = hoaIdList.filter(id => !rejectedIdsToDelete.has(id));
    }

    // DELETING
    try {
      for (const id of acceptedIdsToDelete) {
        const hoaUnitTypeList = await hoaUnitTypeRepository.find({
          where: {
            hoa_id: id,
          },
        });
        if (hoaUnitTypeList.length) {
          for (const hoaUnitType of hoaUnitTypeList) {
            const hoaUnitTypeServiceList = await hoaUnitTypeServiceRepository.find({
              where: {
                hoa_unit_type_id: hoaUnitType.hoa_unit_type_id,
              },
            });
            for (const hoaUnitTypeService of hoaUnitTypeServiceList) {
              await hoaUnitTypeServiceRepository.deleteById(hoaUnitTypeService.hoa_unit_type_service_id, {transaction});
            }
            await hoaUnitTypeRepository.deleteById(hoaUnitType.hoa_unit_type_id, {transaction});
          }
        }
        await this.deleteById(id, {transaction});
      }
      await transaction.commit();
    } catch (error) {
      await transaction.rollback();
      throw error;
    }

    // THROWING ERROR WITH LINKED HOA
    if (rejectedIdsToDelete.size > 0) {
      const hoas = await this.find({
        where: {
          hoa_id: {inq: Array.from(rejectedIdsToDelete)},
        },
      });
      const hoaRejectedNames = hoas?.map(hoa => hoa.name);
      const rejectedNames = JSON.stringify(hoaRejectedNames);
      const error = new CustomError('Hoa has buildings');
      error.code = ErrorCodes.HOAS_HAS_BUILDINGS;
      error.statusCode = 422;
      error.message = rejectedNames;
      throw error;
    }
  }
}
