import { SqlEntityRepository } from '@mikro-orm/postgresql';
import {
  EntityManager,
  Entity,
  MikroORM,
  PrimaryKey,
  Property,
  EventSubscriber,
} from '@mikro-orm/core';
import {
  DockerComposeEnvironment,
  StartedDockerComposeEnvironment,
} from 'testcontainers';
import { v4 as uuid } from 'uuid';

@Entity({ tableName: 'users' })
class User {
  @PrimaryKey()
  id!: number;

  @Property()
  readonly username: string;

  constructor(username: string) {
    this.username = username;
  }
}

class UserSubscriber implements EventSubscriber<User> {
  async afterCreate() {}
  async afterFlush() {}
}

async function getOrm() {
  const userSubscriber = new UserSubscriber();

  const orm = await MikroORM.init({
    type: 'postgresql',
    user: 'postgres',
    password: 'somepassword',
    dbName: 'test_db',
    port: 15432,
    entities: [User],
    subscribers: [userSubscriber],
    // debug: true,
  });

  return { userSubscriber, orm };
}

describe('the tests', () => {
  let environment: StartedDockerComposeEnvironment;
  let orm: MikroORM;
  let em: EntityManager;
  let afterCreateSpy: jest.SpyInstance<Promise<void>>;
  let afterFlushSpy: jest.SpyInstance<Promise<void>>;

  beforeAll(async () => {
    // environment = await new DockerComposeEnvironment(
    //   __dirname,
    //   'docker-compose.yaml'
    // ).up();
  }, 600_000);

  afterAll(async () => {
    // await environment.down();
  });

  beforeEach(async () => {
    const { orm: _orm, userSubscriber } = await getOrm();
    orm = _orm;
    em = orm.em;
    afterCreateSpy = jest.spyOn(userSubscriber, 'afterCreate');
    afterFlushSpy = jest.spyOn(userSubscriber, 'afterFlush');
  });

  afterEach(async () => {
    await orm.close();
  });

  describe('immediate constraint (failures on insert)', () => {
    beforeAll(async () => {
      const { orm } = await getOrm();
      await orm.em.getConnection().execute(
        `
          drop table if exists users;
          create table users (
            id serial primary key,
            username varchar(50) not null constraint unique_username unique deferrable initially immediate
          );
          `
      );
      await orm.close();
    });
    describe('implicit transactions', () => {
      let username: string;
      it('hooks called when inserting a new user succeeds', async () => {
        username = uuid();
        const user = new User(username);
        em.persist(user);
        await em.flush();

        expect(afterCreateSpy).toHaveBeenCalledTimes(1);
        expect(afterFlushSpy).toHaveBeenCalledTimes(1);
      });
      it('hooks not called when inserting a new user fails', async () => {
        const user = new User(username);
        em.persist(user);
        expect(() => em.flush()).rejects.toThrowError(
          /^insert.+duplicate key value/
        );

        expect(afterCreateSpy).toHaveBeenCalledTimes(0);
        expect(afterFlushSpy).toHaveBeenCalledTimes(0);
      });
    });

    describe('explicit transactions with "transactional()"', () => {
      let username: string;
      it('hooks called when inserting a new user succeeds', async () => {
        await em.transactional(async (em) => {
          username = uuid();
          const user = new User(username);
          em.persist(user);
        });

        expect(afterCreateSpy).toHaveBeenCalledTimes(1); // not sure why afterCreat is called twice :/
        expect(afterFlushSpy).toHaveBeenCalledTimes(1);
      });
      it('hooks not called when inserting a new user fails', async () => {
        const work = async () => {
          await em.transactional(async (em) => {
            const user = new User(username);
            em.persist(user);
          });
        };

        expect(work).rejects.toThrowError(/^insert.+duplicate key value/);
        expect(afterCreateSpy).toHaveBeenCalledTimes(0);
        expect(afterFlushSpy).toHaveBeenCalledTimes(0);
      });
    });

    describe('explicit transactions with explicit begin/commit method calls', () => {
      let username: string;
      it('hooks called when inserting a new user succeeds', async () => {
        await em.begin();
        username = uuid();
        const user = new User(username);
        em.persist(user);
        await em.commit();

        expect(afterCreateSpy).toHaveBeenCalledTimes(1);
        expect(afterFlushSpy).toHaveBeenCalledTimes(1);
      });
      it('hooks not called when inserting a new user fails', async () => {
        await em.begin();
        const work = async () => {
          try {
            const user = new User(username);
            em.persist(user);
            await em.commit();
          } catch (error) {
            await em.rollback();
            throw error;
          }
        };

        expect(work).rejects.toThrowError(/^insert.+duplicate key value/);
        expect(afterCreateSpy).toHaveBeenCalledTimes(0);
        expect(afterFlushSpy).toHaveBeenCalledTimes(0);
      });
    });
  });

  describe('deferred constraint (failures on commit)', () => {
    beforeAll(async () => {
      const { orm } = await getOrm();
      await orm.em.getConnection().execute(
        `
        drop table if exists users;
        create table users (
          id serial primary key,
          username varchar(50) not null constraint unique_username unique deferrable initially deferred
        );
          `
      );
      await orm.close();
    });

    describe('implicit transactions', () => {
      let username: string;
      it('hooks called when inserting a new user succeeds', async () => {
        username = uuid();
        const user = new User(username);
        em.persist(user);
        await em.flush();

        expect(afterCreateSpy).toHaveBeenCalledTimes(1);
        expect(afterFlushSpy).toHaveBeenCalledTimes(1);
      });
      it('hooks not called when inserting a new user fails', async () => {
        const user = new User(username);
        em.persist(user);
        expect(() => em.flush()).rejects.toThrowError(
          /^COMMIT.+duplicate key value/
        );

        expect(afterCreateSpy).toHaveBeenCalledTimes(0);
        expect(afterFlushSpy).toHaveBeenCalledTimes(0);
      });
    });

    describe('explicit transactions with "transactional()"', () => {
      let username: string;
      it('hooks called when inserting a new user succeeds', async () => {
        await em.transactional(async (em) => {
          username = uuid();
          const user = new User(username);
          em.persist(user);
        });

        expect(afterCreateSpy).toHaveBeenCalledTimes(1); // not sure why afterCreat is called twice :/
        expect(afterFlushSpy).toHaveBeenCalledTimes(1);
      });
      it('hooks not called when inserting a new user fails', async () => {
        const work = async () => {
          await em.transactional(async (em) => {
            const user = new User(username);
            em.persist(user);
          });
        };

        expect(work).rejects.toThrowError(/^COMMIT.+duplicate key value/);
        expect(afterCreateSpy).toHaveBeenCalledTimes(0);
        expect(afterFlushSpy).toHaveBeenCalledTimes(0);
      });
    });

    describe('explicit transactions with explicit begin/commit method calls', () => {
      let username: string;
      it('hooks called when inserting a new user succeeds', async () => {
        await em.begin();
        username = uuid();
        const user = new User(username);
        em.persist(user);
        await em.commit();

        expect(afterCreateSpy).toHaveBeenCalledTimes(1);
        expect(afterFlushSpy).toHaveBeenCalledTimes(1);
      });
      it('commit should throw when inserting a new user fails', async () => {
        await em.begin();
        const work = async () => {
          try {
            const user = new User(username);
            em.persist(user);
            await em.commit();
          } catch (error) {
            await em.rollback();
            throw error;
          }
        };

        expect(work).rejects.toThrowError(/^COMMIT.+duplicate key value/);
      });
      it('hooks not called when inserting a new user fails', async () => {
        await em.begin();
        const work = async () => {
          try {
            const user = new User(username);
            em.persist(user);
            await em.commit();
          } catch (error) {
            await em.rollback();
            throw error;
          }
        };

        await work(); // should throw but doesn't as demonstrated in previous test case
        expect(afterCreateSpy).toHaveBeenCalledTimes(0);
        expect(afterFlushSpy).toHaveBeenCalledTimes(0);
      });
    });
  });
});
